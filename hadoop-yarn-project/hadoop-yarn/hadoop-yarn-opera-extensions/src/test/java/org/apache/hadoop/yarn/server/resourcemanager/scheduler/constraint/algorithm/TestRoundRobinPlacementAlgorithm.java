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

/**
 * Verify behaviour of {@link RoundRobinPlacementAlgorithm}.
 */
public class TestRoundRobinPlacementAlgorithm {

  private static final int NUM_NODES = 5;
  private static final Resource NODE_CAP = Resource.newInstance(8192, 8);
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