package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.LocalAllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.NodeCandidateSelector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Round-robin / randomised first-fit placement algorithm that rotates the
 * eligible node list on every invocation, avoiding hot-spots while fully
 * respecting YARN resource and placement-constraint checks.
 */
public class RoundRobinPlacementAlgorithm implements ConstraintPlacementAlgorithm {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinPlacementAlgorithm.class);
  private static final int RE_ATTEMPT_COUNT = 2;
  private static final AtomicInteger NEXT_START_INDEX = new AtomicInteger(0);

  private LocalAllocationTagsManager tagsManager;
  private PlacementConstraintManager constraintManager;
  private NodeCandidateSelector nodeSelector;
  private ResourceCalculator resourceCalculator;

  @Override
  public void init(RMContext rmContext) {
    this.tagsManager = new LocalAllocationTagsManager(rmContext.getAllocationTagsManager());
    this.constraintManager = rmContext.getPlacementConstraintManager();
    this.resourceCalculator = rmContext.getScheduler().getResourceCalculator();
    this.nodeSelector = filter -> ((AbstractYarnScheduler) rmContext.getScheduler()).getNodes(filter);
  }

  @Override
  public void place(ConstraintPlacementAlgorithmInput input, ConstraintPlacementAlgorithmOutputCollector collector) {
    BatchedRequests requests = (BatchedRequests) input;
    int placementAttempt = requests.getPlacementAttempt();
    ConstraintPlacementAlgorithmOutput resp = new ConstraintPlacementAlgorithmOutput(requests.getApplicationId());

    List<SchedulerNode> allNodes = new ArrayList<>(nodeSelector.selectNodes(null));
    if (allNodes.isEmpty()) {
      LOG.warn("No nodes available for placement!");
      collector.collect(resp);
      return;
    }
    int startIdx = Math.abs(NEXT_START_INDEX.getAndIncrement()) % allNodes.size();
    Collections.rotate(allNodes, -startIdx);

    List<SchedulingRequest> rejectedRequests = new ArrayList<>();
    Map<NodeId, Resource> availResources = new HashMap<>();

    int rePlacementCount = RE_ATTEMPT_COUNT;
    while (rePlacementCount > 0) {
      doPlacement(requests, resp, allNodes, rejectedRequests, availResources);
      validatePlacement(requests.getApplicationId(), resp, rejectedRequests, availResources);
      if (rejectedRequests.isEmpty() || rePlacementCount == 1) {
        break;
      }
      requests = new BatchedRequests(requests.getIteratorType(), requests.getApplicationId(), rejectedRequests, requests.getPlacementAttempt());
      rejectedRequests = new ArrayList<>();
      rePlacementCount--;
    }

    resp.getRejectedRequests().addAll(rejectedRequests.stream()
        .map(x -> new SchedulingRequestWithPlacementAttempt(placementAttempt, x))
        .collect(Collectors.toList()));
    collector.collect(resp);
    // Safely clean temporary container tags only if they may exist.
    try {
      tagsManager.cleanTempContainers(requests.getApplicationId());
    } catch (NullPointerException npe) {
      // This happens when no temporary tags were recorded for the application
      // during the placement cycle. Swallow the exception because there is
      // nothing to clean up in that case.
      if (LOG.isDebugEnabled()) {
        LOG.debug("No temporary tags to clean for app {}", requests.getApplicationId());
      }
    }
  }

  private boolean attemptPlacementOnNode(ApplicationId appId, Resource available, SchedulingRequest req, SchedulerNode node, boolean ignoreResource) throws InvalidAllocationTagsQueryException {
    boolean fits = ignoreResource || Resources.fitsIn(resourceCalculator, req.getResourceSizing().getResources(), available);
    boolean constraintsOk = PlacementConstraintsUtil.canSatisfyConstraints(appId, req, node, constraintManager, tagsManager);
    return fits && constraintsOk;
  }

  private void doPlacement(BatchedRequests requests, ConstraintPlacementAlgorithmOutput resp, List<SchedulerNode> allNodes, List<SchedulingRequest> rejected, Map<NodeId, Resource> avail) {
    for (SchedulingRequest req : requests) {
      PlacedSchedulingRequest placed = new PlacedSchedulingRequest(req);
      placed.setPlacementAttempt(requests.getPlacementAttempt());
      resp.getPlacedRequests().add(placed);
      int allocs = req.getResourceSizing().getNumAllocations();
      int cursor = 0, checked = 0;
      while (allocs > 0 && checked < allNodes.size()) {
        SchedulerNode node = allNodes.get(cursor);
        cursor = (cursor + 1) % allNodes.size();
        checked++;
        String tagKey = req.getAllocationTags() == null ? "" : req.getAllocationTags().iterator().next();
        if (requests.getBlacklist(tagKey).contains(node.getNodeID())) {
          continue;
        }
        Resource unalloc = avail.computeIfAbsent(node.getNodeID(), n -> Resource.newInstance(node.getUnallocatedResource()));
        try {
          if (attemptPlacementOnNode(requests.getApplicationId(), unalloc, req, node, false)) {
            req.getResourceSizing().setNumAllocations(--allocs);
            Resources.addTo(unalloc, req.getResourceSizing().getResources());
            placed.getNodes().add(node);
            tagsManager.addTempTags(node.getNodeID(), requests.getApplicationId(), req.getAllocationTags());
            checked = 0; // reset scan for next allocation
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("TagManager exception", e);
        }
      }
    }
    requests.getSchedulingRequests().stream()
        .filter(r -> r.getResourceSizing().getNumAllocations() > 0)
        .forEach(r -> rejected.add(cloneReq(r)));
  }

  private void validatePlacement(ApplicationId appId, ConstraintPlacementAlgorithmOutput resp, List<SchedulingRequest> rejected, Map<NodeId, Resource> avail) {
    Iterator<PlacedSchedulingRequest> iter = resp.getPlacedRequests().iterator();
    while (iter.hasNext()) {
      PlacedSchedulingRequest pReq = iter.next();
      Iterator<SchedulerNode> nodeIter = pReq.getNodes().iterator();
      int num = 0;
      while (nodeIter.hasNext()) {
        SchedulerNode node = nodeIter.next();
        try {
          tagsManager.removeTempTags(node.getNodeID(), appId, pReq.getSchedulingRequest().getAllocationTags());
          Resource availOnNode = avail.get(node.getNodeID());
          if (!attemptPlacementOnNode(appId, availOnNode, pReq.getSchedulingRequest(), node, true)) {
            nodeIter.remove();
            num++;
            Resources.subtractFrom(availOnNode, pReq.getSchedulingRequest().getResourceSizing().getResources());
          } else {
            tagsManager.addTempTags(node.getNodeID(), appId, pReq.getSchedulingRequest().getAllocationTags());
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("TagManager exception", e);
        }
      }
      if (num > 0) {
        SchedulingRequest sReq = cloneReq(pReq.getSchedulingRequest());
        sReq.getResourceSizing().setNumAllocations(num);
        rejected.add(sReq);
      }
      if (pReq.getNodes().isEmpty()) {
        iter.remove();
      }
    }
  }

  private static SchedulingRequest cloneReq(SchedulingRequest sReq) {
    return SchedulingRequest.newInstance(sReq.getAllocationRequestId(), sReq.getPriority(), sReq.getExecutionType(), sReq.getAllocationTags(), ResourceSizing.newInstance(sReq.getResourceSizing().getNumAllocations(), sReq.getResourceSizing().getResources()), sReq.getPlacementConstraint());
  }
} 