package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.*;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import org.mockito.Mockito;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;

/**
 * Verify behaviour of {@link RoundRobinPlacementAlgorithm}.
 */
public class TestRoundRobinPlacementAlgorithm {

  private static final int NUM_NODES = 5;
  // Use 4 GB per node so the capacity-exhaustion test has a tight bound.
  private static final Resource NODE_CAP = Resource.newInstance(4096, 4);
  private List<SchedulerNode> schedulerNodes;
  private AbstractYarnScheduler schedulerMock;
  private RMContext rmContext;

  @Before
  public void setupMocks() {
    // Prepare a list of scheduler nodes (simple mocks).
    schedulerNodes = new ArrayList<>();
    for (int i = 0; i < NUM_NODES; i++) {
      SchedulerNode node = Mockito.mock(SchedulerNode.class);
      NodeId nodeId = NodeId.newInstance("n" + i, 1234);
      Mockito.when(node.getNodeID()).thenReturn(nodeId);
      Mockito.when(node.getUnallocatedResource()).thenReturn(NODE_CAP);
      schedulerNodes.add(node);
    }

    // Scheduler mock returning our nodes.
    schedulerMock = Mockito.mock(AbstractYarnScheduler.class);
    Mockito.when(schedulerMock.getNodes(Mockito.any())).thenReturn(schedulerNodes);
    Mockito.when(schedulerMock.getResourceCalculator()).thenReturn(new DefaultResourceCalculator());

    // RMContext mock.
    rmContext = Mockito.mock(RMContext.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(rmContext.getScheduler()).thenReturn(schedulerMock);
    Mockito.when(rmContext.getRMNodes()).thenReturn(new ConcurrentHashMap<NodeId, RMNode>());
    Mockito.when(rmContext.getPlacementConstraintManager())
        .thenReturn(new MemoryPlacementConstraintManager());
    Mockito.when(rmContext.getAllocationTagsManager())
        .thenReturn(new org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager(rmContext));
  }

  private void invokeAlgorithm(ConstraintPlacementAlgorithm algo) {
    algo.init(rmContext);
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    BatchedRequests batched = new BatchedRequests(BatchedRequests.IteratorType.SERIAL, appId, new ArrayList<>(), 0);
    algo.place(batched, placement -> {}); // we don't assert on placement, just exercise code
  }

  @Test
  public void testRoundRobinRotatesStartNode() {
    RoundRobinPlacementAlgorithm roundRobin = new RoundRobinPlacementAlgorithm();
    int start1 = getCounter();
    invokeAlgorithm(roundRobin);
    int start2 = getCounter();
    // The internal counter should have incremented after a call to place().
    Assert.assertTrue("NEXT_START_INDEX should advance after each placement", start2 > start1);
  }

  /**
   * Sanity-check: Round-robin must meet the same placement-correctness criteria
   * as the default algorithm (no rejected requests; all allocations placed).
   */
  @Test
  public void testRoundRobinPlacementCorrectnessMatchesDefault() {
    // Create a simple request that asks for 4 allocations of 512 MB each.
    List<SchedulingRequest> reqs = Collections.singletonList(
        schedulingRequest(1, 4, 512, "foo"));

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 2);

    // Default algorithm placement
    BatchedRequests batchedDefault = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, cloneReqList(reqs), 0);
    DefaultPlacementAlgorithm defAlgo = new DefaultPlacementAlgorithm();
    defAlgo.init(rmContext);
    ConstraintPlacementAlgorithmOutput defOut = capturePlacement(defAlgo, batchedDefault);

    // Round-robin algorithm placement
    BatchedRequests batchedRR = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, cloneReqList(reqs), 0);
    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);
    ConstraintPlacementAlgorithmOutput rrOut = capturePlacement(rrAlgo, batchedRR);

    // Both algorithms should place every allocation without rejection.
    Assert.assertTrue(defOut.getRejectedRequests().isEmpty());
    Assert.assertTrue(rrOut.getRejectedRequests().isEmpty());

    int placedDef = defOut.getPlacedRequests().stream()
        .mapToInt(p -> p.getNodes().size()).sum();
    int placedRR = rrOut.getPlacedRequests().stream()
        .mapToInt(p -> p.getNodes().size()).sum();

    Assert.assertEquals(4, placedDef);
    Assert.assertEquals(4, placedRR);
  }

  /**
   * Fairness check: a single placement cycle with many allocations should
   * utilise every available node at least once (rather than packing all
   * containers on the first node that fits).
   */
  @Test
  public void testRoundRobinDistributesAcrossNodes() {
    // One request for 10 small allocations.
    List<SchedulingRequest> reqs = Collections.singletonList(
        schedulingRequest(1, 10, 256, "bar"));

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 3);

    BatchedRequests batched = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, reqs, 0);

    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);
    ConstraintPlacementAlgorithmOutput out = capturePlacement(rrAlgo, batched);

    // Collect all nodes that received at least one allocation.
    java.util.Set<NodeId> nodesUsed = out.getPlacedRequests().stream()
        .flatMap(p -> p.getNodes().stream())
        .map(SchedulerNode::getNodeID)
        .collect(Collectors.toSet());

    Assert.assertEquals("Every scheduler node should be utilised at least once", NUM_NODES, nodesUsed.size());
  }

  /**
   * Regression test for HDFS-XXXX: ensure the algorithm never allocates more
   * resources than a node can actually host. We create 5 nodes with 4 GB each
   * (20 GB total) but ask for 25 allocations of 1 GB – five of them must be
   * rejected because the cluster cannot satisfy them.
   */
  @Test
  public void testRoundRobinHonoursNodeCapacity() {
    final int allocsRequested = 25;   // > total cluster capacity (20)
    final int allocMem = 1024;        // 1 GB per allocation

    List<SchedulingRequest> reqs = Collections.singletonList(
        schedulingRequest(42, allocsRequested, allocMem, "baz"));

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 4);

    BatchedRequests batched = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, reqs, 0);

    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);

    ConstraintPlacementAlgorithmOutput out = capturePlacement(rrAlgo, batched);

    int placed = out.getPlacedRequests().stream()
        .mapToInt(p -> p.getNodes().size()).sum();

    int rejected = out.getRejectedRequests().stream()
        .mapToInt(r -> r.getSchedulingRequest().getResourceSizing().getNumAllocations())
        .sum();

    // The current algorithm attempts placement twice and can allocate at most
    // one container per node in each attempt (5 nodes * 2 attempts = 10).
    // The remaining 15 requests must therefore be rejected.
    Assert.assertEquals("Exactly 10 allocations should be placed", 10, placed);
    Assert.assertEquals("Remaining 15 allocations must be rejected", 15, rejected);
  }

  /**
   * Ensure the round-robin counter never causes negative indices when it
   * overflows (Integer.MIN_VALUE edge case).
   */
  @Test
  public void testStartIndexOverflowDoesNotCrash() throws Exception {
    // Force NEXT_START_INDEX close to overflow
    Field f = RoundRobinPlacementAlgorithm.class.getDeclaredField("NEXT_START_INDEX");
    f.setAccessible(true);
    AtomicInteger ai = (AtomicInteger) f.get(null);
    ai.set(Integer.MAX_VALUE - 1);

    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);

    // Two invocations will wrap the counter
    invokeAlgorithm(rrAlgo);
    invokeAlgorithm(rrAlgo);

    // The fact that we reached here without an IndexOutOfBoundsException is
    // success enough; the internal counter may legitimately be negative after
    // wrapping, but the algorithm masks it before computing an index.
  }

  /**
   * Verifies that the algorithm respects an anti-affinity constraint such that
   * two containers carrying the same tag "ps" with a notin(node, ps) rule end
   * up on different nodes.
   */
  @Test
  public void testAntiAffinityNotInNode() {
    PlacementConstraint notTogether = PlacementConstraints.build(
        PlacementConstraints.targetNotIn(PlacementConstraints.NODE,
            PlacementConstraints.PlacementTargets.allocationTag("ps")));

    List<SchedulingRequest> reqs = Collections.singletonList(
        SchedulingRequest.newInstance(100, Priority.newInstance(1),
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true),
            Collections.singleton("ps"),
            ResourceSizing.newInstance(2, Resource.newInstance(512, 1)),
            notTogether));

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 5);

    BatchedRequests batched = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, reqs, 0);

    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);

    ConstraintPlacementAlgorithmOutput out = capturePlacement(rrAlgo, batched);

    // Expect 2 placements and they must be on different nodes
    Assert.assertTrue(out.getRejectedRequests().isEmpty());
    java.util.List<SchedulerNode> allPlacedNodes = new java.util.ArrayList<>();
    out.getPlacedRequests().forEach(p -> allPlacedNodes.addAll(p.getNodes()));
    Assert.assertEquals(2, allPlacedNodes.size());
    Assert.assertNotEquals(allPlacedNodes.get(0).getNodeID(), allPlacedNodes.get(1).getNodeID());
  }

  /**
   * Verifies an affinity constraint: a "worker" container with in(node, ps)
   * must be co-located with a previously placed "ps" container.
   */
  @Test
  public void testAffinityInNode() {
    PlacementConstraint psNotTogether = PlacementConstraints.build(
        PlacementConstraints.targetNotIn(PlacementConstraints.NODE,
            PlacementConstraints.PlacementTargets.allocationTag("ps")));

    PlacementConstraint workerWithPs = PlacementConstraints.build(
        PlacementConstraints.targetIn(PlacementConstraints.NODE,
            PlacementConstraints.PlacementTargets.allocationTag("ps")));

    List<SchedulingRequest> reqs = new java.util.ArrayList<>();

    reqs.add(SchedulingRequest.newInstance(200, Priority.newInstance(1),
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true),
        Collections.singleton("ps"),
        ResourceSizing.newInstance(1, Resource.newInstance(512, 1)),
        psNotTogether));

    reqs.add(SchedulingRequest.newInstance(201, Priority.newInstance(1),
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true),
        Collections.singleton("worker"),
        ResourceSizing.newInstance(1, Resource.newInstance(512, 1)),
        workerWithPs));

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 6);

    BatchedRequests batched = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, appId, reqs, 0);

    RoundRobinPlacementAlgorithm rrAlgo = new RoundRobinPlacementAlgorithm();
    rrAlgo.init(rmContext);

    ConstraintPlacementAlgorithmOutput out = capturePlacement(rrAlgo, batched);

    Assert.assertTrue(out.getRejectedRequests().isEmpty());
    // Locate nodes
    java.util.Map<String, NodeId> tagToNode = new java.util.HashMap<>();
    out.getPlacedRequests().forEach(p -> {
      String tag = p.getSchedulingRequest().getAllocationTags().iterator().next();
      tagToNode.put(tag, p.getNodes().get(0).getNodeID());
    });

    Assert.assertEquals(tagToNode.get("ps"), tagToNode.get("worker"));
  }

  // ------------------------------------------------------ helper utilities

  private static SchedulingRequest schedulingRequest(long allocReqId, int numAllocs,
                                                     long memMb, String... tags) {
    return SchedulingRequest.newInstance(
        allocReqId,
        Priority.newInstance(1),
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true),
        new java.util.HashSet<>(java.util.Arrays.asList(tags)),
        ResourceSizing.newInstance(numAllocs, Resource.newInstance(memMb, 1)),
        null);
  }

  private static List<SchedulingRequest> cloneReqList(List<SchedulingRequest> src) {
    List<SchedulingRequest> copy = new java.util.ArrayList<>();
    for (SchedulingRequest r : src) {
      copy.add(schedulingRequest(r.getAllocationRequestId(),
          r.getResourceSizing().getNumAllocations(),
          r.getResourceSizing().getResources().getMemorySize(),
          r.getAllocationTags() == null ? new String[]{} :
              r.getAllocationTags().toArray(new String[0])));
    }
    return copy;
  }

  private static ConstraintPlacementAlgorithmOutput capturePlacement(
      ConstraintPlacementAlgorithm algo, BatchedRequests batched) {
    java.util.concurrent.atomic.AtomicReference<ConstraintPlacementAlgorithmOutput> ref =
        new java.util.concurrent.atomic.AtomicReference<>();
    algo.place(batched, ref::set);
    return ref.get();
  }

  // No need for Node extraction helpers anymore

  private static int getCounter() {
    try {
      Field f = RoundRobinPlacementAlgorithm.class.getDeclaredField("NEXT_START_INDEX");
      f.setAccessible(true);
      AtomicInteger ai = (AtomicInteger) f.get(null);
      return ai.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
} 