package controllers

import (
	"context"
	"time"

	v1 "github.com/azure/eviction-autoscaler/api/v1"
	"github.com/azure/eviction-autoscaler/internal/namespacefilter"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// These tests exercise the reconcile-level PDB-floor pin/restore/bail wiring.
// envtest does not run the disruption controller, so pdb.Status is populated
// manually to drive the state machine.
var _ = Describe("PDB floor pin/restore/bail", func() {
	ctx := context.Background()
	const name = "floor-ea"

	var (
		ns            string
		nsName        types.NamespacedName
		reconciler    *EvictionAutoScalerReconciler
		selectorMatch = map[string]string{"app": "floor-test"}
	)

	BeforeEach(func() {
		// The master switch defaults to off (feature ships dormant); enable it for
		// these specs. Harmless to other specs, which leave it off.
		pdbFloorMutationEnabled = true
		nsObj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "floor",
				Annotations: map[string]string{
					namespacefilter.EnableEvictionAutoscalerAnnotationKey: "true",
				},
			},
		}
		Expect(k8sClient.Create(ctx, nsObj)).To(Succeed())
		ns = nsObj.Name
		nsName = types.NamespacedName{Name: name, Namespace: ns}

		reconciler = &EvictionAutoScalerReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Filter: &evictionTestFilter{},
		}
	})

	createDeployment := func(replicas int32, maxSurge intstr.IntOrString) *appsv1.Deployment {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: selectorMatch},
				Strategy: appsv1.DeploymentStrategy{
					RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &maxSurge},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: selectorMatch},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "nginx", Image: "nginx:latest"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		return dep
	}

	setPDBStatus := func(pdb *policyv1.PodDisruptionBudget, disruptionsAllowed, currentHealthy, desiredHealthy, expected int32) {
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, pdb)).To(Succeed())
		pdb.Status = policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: disruptionsAllowed,
			CurrentHealthy:     currentHealthy,
			DesiredHealthy:     desiredHealthy,
			ExpectedPods:       expected,
			ObservedGeneration: pdb.Generation,
		}
		Expect(k8sClient.Status().Update(ctx, pdb)).To(Succeed())
	}

	addCordonedPods := func(n int) {
		for i := 0; i < n; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "floor-pod-", Namespace: ns, Labels: selectorMatch},
				Spec:       corev1.PodSpec{NodeName: "floor-node-" + ns, Containers: []corev1.Container{{Name: "nginx", Image: "nginx:latest"}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		}
	}

	cordonWithPods := func(n int) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "floor-node-" + ns},
			Spec:       corev1.NodeSpec{Unschedulable: true},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		addCordonedPods(n)
	}

	// makeMaxUnavailablePDB creates a maxUnavailable:1 PDB (DA==0) whose floor at
	// 5 baseline replicas is 4.
	makeBlockedPDB := func() *policyv1.PodDisruptionBudget {
		mu := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &mu,
				Selector:       &metav1.LabelSelector{MatchLabels: selectorMatch},
			},
		}
		Expect(k8sClient.Create(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 0, 4, 4, 5)
		return pdb
	}

	// surgeAndPin drives the initial reconcile (baseline) then the surge reconcile,
	// leaving the PDB pinned to minAvailable=4 and the deployment surged to 7.
	surgeAndPin := func(pdb *policyv1.PodDisruptionBudget) *v1.EvictionAutoScaler {
		ea := &v1.EvictionAutoScaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       v1.EvictionAutoScalerSpec{TargetName: name, TargetKind: "deployment"},
		}
		Expect(k8sClient.Create(ctx, ea)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		// Eviction time in the past so the later scale-down cooldown check passes.
		ea.Spec.LastEviction = v1.Eviction{PodName: "p", EvictionTime: metav1.NewTime(time.Now().Add(-2 * cooldown))}
		Expect(k8sClient.Update(ctx, ea)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable).To(BeNil())
		Expect(pdb.Spec.MinAvailable).NotTo(BeNil())
		Expect(pdb.Spec.MinAvailable.IntVal).To(Equal(int32(4)))
		Expect(pdb.Annotations).To(HaveKey(AnnotationOriginalPDBSpec))
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).NotTo(BeNil())
		Expect(*ea.Status.PinnedPDBFloor).To(Equal(int32(4)))
		return ea
	}

	It("pins on surge, widens DisruptionsAllowed, and restores the partner PDB exactly on scale-down", func() {
		createDeployment(5, intstr.FromInt32(2))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb)

		// Deployment surged to 7 so the absolute floor of 4 now yields headroom
		// (7-4=3 disruptions worth) instead of tracking the surged count.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(7)))

		// Drain finishes: disruptions allowed again.
		setPDBStatus(pdb, 1, 7, 4, 7)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// PDB restored to the partner's exact original (maxUnavailable:1), annotations gone.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))

		// Deployment scaled back to baseline, pin cleared.
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(5)))
		ea := &v1.EvictionAutoScaler{}
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())
	})

	It("bails and restores the partner PDB on an external replica change mid-pin", func() {
		createDeployment(5, intstr.FromInt32(2))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb)

		// An external actor changes the deployment replicas out from under us
		// (not via ApplySurge, so the recorded surge annotation still reads 7).
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		dep.Spec.Replicas = ptr.To(int32(10))
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// The bail restored the partner PDB and cleared the pin.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		ea := &v1.EvictionAutoScaler{}
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())
	})

	It("does not oscillate when replicas are parked above the recorded surge without a generation bump", func() {
		createDeployment(5, intstr.FromInt32(2))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb) // pinned minAvailable:4, surged to 7, recorded surge annotation=7

		// Park replicas above the recorded surge via the /scale subresource, which does
		// NOT bump the Deployment generation — reproducing the HPA/KEDA-style scale-up
		// where the generation-reset path does not fire and RecordedSurge stays < live.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		scale := &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       autoscalingv1.ScaleSpec{Replicas: 10},
		}
		Expect(k8sClient.SubResource("scale").Update(ctx, dep, client.WithSubResourceBody(scale))).To(Succeed())

		// The first reconcile bails (restores the partner PDB + un-pins); subsequent
		// reconciles must NOT re-pin. Before the fix this flip-flopped every cycle.
		for i := 0; i < 3; i++ {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
			Expect(err).NotTo(HaveOccurred())
		}

		// The PDB stays restored to the partner's original (maxUnavailable:1), pin cleared.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))
		ea := &v1.EvictionAutoScaler{}
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())
	})

	It("honors a mid-drain PDB edit by re-pinning a floor derived from the new spec, and restores the edit on completion", func() {
		createDeployment(5, intstr.FromInt32(5)) // maxSurge:5 so the surge is not capped at 7
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb) // pins minAvailable:4, surges to 7, snapshot = {maxUnavailable:1}

		// Partner loosens their PDB mid-drain (maxUnavailable:1 -> 2), overwriting our pin
		// but leaving our tracking annotations; the drain is still blocking.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		pdb.Spec.MinAvailable = nil
		pdb.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(2))
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 0, 7, 4, 7)

		// Grow the displaced count so surgeTarget (5+4=9) exceeds current replicas (7),
		// routing the reconcile THROUGH pinFloorBeforeSurge -> ensurePDBFloor with the
		// edit live. Without this, GetReplicas()>=surgeTarget early-returns before it runs.
		addCordonedPods(2)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Option A honor: re-pinned to an absolute floor derived from the NEW spec at the
		// frozen baseline (maxUnavailable:2 at 5 replicas -> floor 3), and re-snapshotted.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable).To(BeNil())
		Expect(pdb.Spec.MinAvailable).NotTo(BeNil())
		Expect(pdb.Spec.MinAvailable.IntVal).To(Equal(int32(3)))
		Expect(pdb.Annotations[AnnotationPinnedFloor]).To(Equal("3"))
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(9)))
		ea := &v1.EvictionAutoScaler{}
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(*ea.Status.PinnedPDBFloor).To(Equal(int32(3)))

		// Drain finishes: the partner's LAST edit (maxUnavailable:2) is restored — not the
		// pre-edit original (maxUnavailable:1) — and the deployment scales back to baseline.
		setPDBStatus(pdb, 1, 9, 3, 9)
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(2)))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))
		Expect(k8sClient.Get(ctx, nsName, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(5)))
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())
	})

	It("honors a mid-drain PDB tighten, re-pinning a higher floor (percent spec)", func() {
		createDeployment(5, intstr.FromInt32(5))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb) // pin minAvailable:4, snapshot {maxUnavailable:1}

		// Partner tightens to minAvailable:90% (ceil(90% of 5) = 5 at the frozen baseline).
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		pdb.Spec.MaxUnavailable = nil
		pdb.Spec.MinAvailable = ptr.To(intstr.FromString("90%"))
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 0, 7, 4, 7)
		addCordonedPods(2)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Re-pinned to the higher floor 5, derived from the percent spec at baseline.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).NotTo(BeNil())
		Expect(pdb.Spec.MinAvailable.Type).To(Equal(intstr.Int))
		Expect(pdb.Spec.MinAvailable.IntVal).To(Equal(int32(5)))
		Expect(pdb.Annotations[AnnotationPinnedFloor]).To(Equal("5"))

		// Completion restores the partner's exact percent spec.
		setPDBStatus(pdb, 1, 9, 5, 9)
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).NotTo(BeNil())
		Expect(pdb.Spec.MinAvailable.Type).To(Equal(intstr.String))
		Expect(pdb.Spec.MinAvailable.StrVal).To(Equal("90%"))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))
	})

	It("does not re-pin (or thrash) when a controller re-asserts the already-snapshotted spec", func() {
		createDeployment(5, intstr.FromInt32(5))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb) // snapshot = {maxUnavailable:1}, pin minAvailable:4

		// A GitOps controller reverts the PDB to the partner's declared spec — exactly the
		// spec we captured at first pin (maxUnavailable:1).
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		pdb.Spec.MinAvailable = nil
		pdb.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(1))
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 0, 7, 4, 7)
		addCordonedPods(2)

		// Repeated reconciles must yield to the re-asserted spec (no re-pin) rather than
		// flip-flop the PDB every cycle.
		for i := 0; i < 3; i++ {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
			Expect(pdb.Spec.MinAvailable).To(BeNil())
			Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		}
	})

	It("does not re-pin a mid-drain edit whose derived floor is non-positive", func() {
		createDeployment(5, intstr.FromInt32(5))
		pdb := makeBlockedPDB()
		cordonWithPods(2)

		surgeAndPin(pdb)

		// Partner edits to maxUnavailable:5 -> desiredHealthy at the baseline of 5 is 0.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		pdb.Spec.MinAvailable = nil
		pdb.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(5))
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 0, 7, 4, 7)
		addCordonedPods(2)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// Nothing to protect at floor 0 -> no re-pin; the partner's spec is preserved.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(5)))
	})

	It("skips first-capture pinning when the workload is above its baseline (autoscaler above min)", func() {
		// An autoscaler (HPA/KEDA) running above its min: Status.MinReplicas is the
		// configured min (5) while the live desired replica count is higher (7).
		// Deriving the floor at 5 would leave the pinned PDB over-permissive, so the
		// first-capture guard must skip pinning entirely and leave the partner PDB
		// untouched (the drain proceeds under the partner's own relative PDB).
		mu := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &mu,
				Selector:       &metav1.LabelSelector{MatchLabels: selectorMatch},
			},
		}
		ea := &v1.EvictionAutoScaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Status:     v1.EvictionAutoScalerStatus{MinReplicas: 5},
		}

		floor, pinned, err := reconciler.ensurePDBFloor(ctx, ea, pdb, 7) // live 7 != baseline 5
		Expect(err).NotTo(HaveOccurred())
		Expect(pinned).To(BeFalse())
		Expect(floor).To(Equal(int32(0)))

		// Partner PDB left exactly as-is; no pin spec change, no tracking annotations.
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())

		// Sanity (positive control): the same PDB DOES have a non-zero floor at the
		// baseline (5) — so it is the guard, not a zero floor, that gates the skip.
		dh, err := desiredHealthyAt(pdb.Spec, 5)
		Expect(err).NotTo(HaveOccurred())
		Expect(dh).To(Equal(int32(4))) // maxUnavailable:1 at 5 replicas -> keep 4
	})

	It("restores a lingering pin on the no-scale completion path", func() {
		// Aftermath of a partial cleanup: a prior drain pinned the PDB (minAvailable:4
		// + our tracking annotations) but the restore never completed, and the
		// Deployment is back at baseline. With disruptions allowed again and a
		// past-cooldown eviction, the reconcile reaches the no-scale tail — which must
		// restore the held pin (before this hardening it would linger, since a
		// status-only update does not re-trigger a reconcile).
		createDeployment(5, intstr.FromInt32(2))

		ma := intstr.FromInt32(4)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: ns,
				Annotations: map[string]string{
					AnnotationOriginalPDBSpec: `{"maxUnavailable":1}`, // partner's original spec
					AnnotationPinnedFloor:     "4",
				},
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &ma, // our pin
				Selector:     &metav1.LabelSelector{MatchLabels: selectorMatch},
			},
		}
		Expect(k8sClient.Create(ctx, pdb)).To(Succeed())
		setPDBStatus(pdb, 1, 5, 4, 5) // disruptions allowed again, at baseline

		ea := &v1.EvictionAutoScaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       v1.EvictionAutoScalerSpec{TargetName: name, TargetKind: "deployment"},
		}
		Expect(k8sClient.Create(ctx, ea)).To(Succeed())

		// First reconcile initializes MinReplicas/TargetGeneration (pin left intact).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// An unhandled, past-cooldown eviction so the reconcile falls through to the
		// no-scale tail (DA>0, cooldown elapsed, replicas == MinReplicas).
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		ea.Spec.LastEviction = v1.Eviction{PodName: "p", EvictionTime: metav1.NewTime(time.Now().Add(-2 * cooldown))}
		Expect(k8sClient.Update(ctx, ea)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())

		// The no-scale tail restored the partner's PDB and cleared our tracking.
		Expect(k8sClient.Get(ctx, nsName, pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable).To(BeNil())
		Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
		Expect(pdb.Spec.MaxUnavailable.IntVal).To(Equal(int32(1)))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationOriginalPDBSpec))
		Expect(pdb.Annotations).NotTo(HaveKey(AnnotationPinnedFloor))
		Expect(k8sClient.Get(ctx, nsName, ea)).To(Succeed())
		Expect(ea.Status.PinnedPDBFloor).To(BeNil())
	})
})
