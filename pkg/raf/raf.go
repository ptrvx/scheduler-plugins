package raf

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	latencyFactor = 2000.00
	Name          = "RafPlugin"
	AppGroupLabel = "appgroup.raf.rs"
	MembersLabel  = "members.raf.rs"
	ModeLabel     = "mode.raf.rs"
	UploadLabel   = "upload.raf.rs"
	DownloadLabel = "download.raf.rs"
	LatencyLabel  = "latency.raf.rs"
)

type RafPlugin struct {
	appGroup      *AppGroup
	clientset     *kubernetes.Clientset
	dynamicClient dynamic.Interface
}

func (p *RafPlugin) Name() string {
	return Name
}

// Store Mode and AppGroup in CycleState during PreFilter
func (p *RafPlugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) *framework.Status {
	mode := UnknownMode
	download, ok := pod.Annotations[DownloadLabel]
	if ok {
		klog.InfoS("Download threshold", "download", download)
		mode |= DownloadMode
	}
	upload, ok := pod.Annotations[UploadLabel]
	if ok {
		klog.InfoS("Upload threshold", "upload", upload)
		mode |= UploadMode
	}
	latency, ok := pod.Annotations[LatencyLabel]
	if ok {
		klog.InfoS("Latency threshold", "latency", latency)
		mode |= LatencyMode
	}
	klog.InfoS("Priority mode", "mode", mode)
	state.Write(framework.StateKey(ModeLabel), mode)

	group := p.detectGroupMembership(pod)
	if group == "" {
		return framework.NewStatus(framework.Success)
	}
	klog.InfoS("Group membership", "appgroup", group)
	state.Write(framework.StateKey(AppGroupLabel), StringState(group))

	return framework.NewStatus(framework.Success)
}

func (p *RafPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	modeState, err := state.Read(framework.StateKey(ModeLabel))
	if err != nil {
		klog.Errorf("pod mode not found: %v", err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod mode not found")
	}
	mode, ok := modeState.(mode)
	if !ok {
		klog.Errorf("failed to read mod state: %v", err)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Failed to read mod state")
	}
	klog.InfoS("Priority mode", "mode", mode)

	gvr := schema.GroupVersionResource{
		Group:    "raf.rs",
		Version:  "v1",
		Resource: "nodemetrics",
	}

	// Retrieve group information from CycleState
	// groupName := ""
	// group, err := state.Read(framework.StateKey(AppGroupLabel))
	// if errors.Is(err, framework.ErrNotFound) {
	// 	group = nil
	// } else if err != nil {
	// 	// If there's no group, proceed with normal scheduling
	// 	return framework.NewStatus(framework.Success)
	// }

	// if group != nil {
	// 	name, ok := group.(StringState)
	// 	if !ok {
	// 		return framework.NewStatus(framework.Unschedulable, "Invalid group name found")
	// 	}
	// 	groupName = string(name)
	// }

	// Check if we want to use all nodes or just AppGroup members

	// Fetch the node list associated with this group
	// members, err := state.Read(framework.StateKey(MembersLabel))
	// if err != nil {
	// 	return framework.NewStatus(framework.Error, "Failed to retrieve group nodes")
	// }

	// var groupNodes AppGroupNodes
	// groupNodes, ok := members.(AppGroupNodes)
	// if !ok {
	// 	return framework.NewStatus(framework.Error, "Invalid data type for group nodes")
	// }

	// Get cluster nodes list
	nodes, err := getNodeItems(ctx, p.clientset)
	if err != nil {
		klog.Errorf("failed to fetch node IPs: %v", err)
		return framework.NewStatus(framework.Error, "Failed to fetch node IPs")
	}
	// In case group has entries fetch nodes from AppGroup instead

	var downloadThreshold float64
	var uploadThreshold float64
	var latencyThreshold float64

	if mode&UploadMode != 0 {
		uploadThreshold, err = strconv.ParseFloat(pod.Annotations[UploadLabel], 64)
		if err != nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Failed to parse upload")
		}
	}
	if mode&DownloadMode != 0 {
		downloadThreshold, err = strconv.ParseFloat(pod.Annotations[DownloadLabel], 64)
		if err != nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Failed to parse download")
		}
	}
	if mode&LatencyMode != 0 {
		latencyThreshold, err = strconv.ParseFloat(pod.Annotations[LatencyLabel], 64)
		if err != nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Failed to parse latency")
		}
	}

	nodeMetric, err := p.dynamicClient.Resource(gvr).Namespace(pod.Namespace).Get(ctx, nodeInfo.Node().Name, metav1.GetOptions{})
	if err != nil {
		// TODO: PostFilter re-schedule
		klog.Errorf("failed to get NodeMetric %v: %v", nodeInfo.Node().Name, err)
		return framework.NewStatus(framework.Unschedulable, "Node metric not found")
	}

	metrics, found, err := unstructured.NestedMap(nodeMetric.Object, "spec", "metrics")
	if err != nil {
		klog.Errorf("failed to retreive metrics from NodeMetric: %v", err)
		return framework.NewStatus(framework.Unschedulable, "failed to retreive metrics from NodeMetric")
	}
	if !found || metrics == nil {
		klog.Errorf("metrics not found")
		return framework.NewStatus(framework.Unschedulable, "metrics not found")
	}

	// Iterate through the group nodes and check network requirements
	for _, node := range nodes {
		valuesRaw, ok := metrics[node.Name]
		if !ok {
			klog.Errorf("metrics for node %v not found", node.Name)
			return framework.NewStatus(framework.Unschedulable, "metrics for node not found")
		}

		values, ok := valuesRaw.(map[string]interface{})
		if !ok {
			klog.Errorf("failed to assert metrics format for node %v to map", node.Name)
			return framework.NewStatus(framework.Unschedulable, "invalid metrics format")
		}
		upload, uploadOk := values["upload"].(float64)
		download, downloadOk := values["download"].(float64)
		latency, latencyOk := values["latency"].(float64)

		if mode&UploadMode != 0 {
			if !uploadOk || upload < uploadThreshold {
				klog.Errorf("node %v does not meet upload requirements", node.Name)
				return framework.NewStatus(framework.Unschedulable, "Node does not meet upload requirements")
			}
		}
		if mode&DownloadMode != 0 {
			if !downloadOk || download < downloadThreshold {
				klog.Errorf("node %v does not meet download requirements", node.Name)
				return framework.NewStatus(framework.Unschedulable, "Node does not meet download requirements")
			}
		}
		if mode&LatencyMode != 0 {
			if !latencyOk || latency > latencyThreshold {
				klog.Errorf("node %v does not meet latency requirements", node.Name)
				return framework.NewStatus(framework.Unschedulable, "Node does not meet latency requirements")
			}
		}
	}

	return framework.NewStatus(framework.Success, "Node is valid")
}

func (p *RafPlugin) PostFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	klog.InfoS("PostFilter invoked", "pod", klog.KObj(pod))

	for nodeName, status := range filteredNodeStatusMap {
		if status.Code() == framework.Unschedulable {
			klog.Infof("Node %v is unschedulable, considering other nodes", nodeName)
		}
	}

	return nil, framework.NewStatus(framework.Success, "PostFilter completed successfully")
}

func (p *RafPlugin) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (int64, *framework.Status) {
	klog.InfoS("Scoring node", "pod", klog.KObj(pod), "node", nodeName)

	modeState, err := state.Read(framework.StateKey(ModeLabel))
	if err != nil {
		klog.Errorf("pod mode not found: %v", err)
		return 0, framework.NewStatus(framework.Unschedulable, "pod mod not found")
	}
	mode, ok := modeState.(mode)
	if !ok {
		klog.Errorf("failed to read mod state: %v", err)
		return 0, framework.NewStatus(framework.Unschedulable, "failed to read mod state")
	}
	klog.InfoS("Priority mode", "mode", mode)

	gvr := schema.GroupVersionResource{
		Group:    "raf.rs",
		Version:  "v1",
		Resource: "nodemetrics",
	}

	nodeMetric, err := p.dynamicClient.Resource(gvr).Namespace(pod.Namespace).Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("failed to get NodeMetric for node %v: %v", nodeName, err)
		return 0, framework.NewStatus(framework.Error, "Node metric not found")
	}

	metrics, found, err := unstructured.NestedMap(nodeMetric.Object, "spec", "metrics")
	if err != nil || !found || metrics == nil {
		klog.Errorf("metrics not found or failed to retrieve for node %v", nodeName)
		return 0, framework.NewStatus(framework.Unschedulable, "metrics not found or failed to retrieve")
	}

	// Simplified scoring logic
	valuesRaw, ok := metrics[nodeName]
	if !ok {
		klog.Errorf("metrics for node %v not found", nodeName)
		return 0, framework.NewStatus(framework.Unschedulable, "metrics for node not found")
	}

	values, ok := valuesRaw.(map[string]interface{})
	if !ok {
		klog.Errorf("failed to assert metrics format for node %v to map", nodeName)
		return 0, framework.NewStatus(framework.Unschedulable, "invalid metrics format")
	}

	// Calculate score based on your criteria (upload, download, latency)
	var score int64 = 0

	upload, uploadOk := values["upload"].(float64)
	download, downloadOk := values["download"].(float64)
	latency, latencyOk := values["latency"].(float64)

	if uploadOk && mode&UploadMode != 0 {
		score += int64(upload) // Example scoring: the higher the upload speed, the better
	}
	if downloadOk && mode&DownloadMode != 0 {
		score += int64(download) // Example scoring: the higher the download speed, the better
	}
	if latencyOk && mode&LatencyMode != 0 {
		score -= int64(latency * latencyFactor) // Example scoring: the lower the latency, the better
	}

	return score, framework.NewStatus(framework.Success, fmt.Sprintf("Scored node %v with score %v", nodeName, score))
}

func (p *RafPlugin) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, scores framework.NodeScoreList) *framework.Status {
	var maxScore int64 = 0
	for _, nodeScore := range scores {
		if nodeScore.Score > maxScore {
			maxScore = nodeScore.Score
		}
	}

	if maxScore == 0 {
		return framework.NewStatus(framework.Success, "All nodes scored 0")
	}

	for i := range scores {
		scores[i].Score = scores[i].Score * 100 / maxScore
	}

	return framework.NewStatus(framework.Success, "Scores normalized")
}

func (p *RafPlugin) ScoreExtensions() framework.ScoreExtensions {
	return p
}

func New(_ context.Context, _ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	clientset, dynamicClient, err := getKubernetesClient()
	if err != nil {
		klog.Errorf("failed to initialize k8s clients: %v", err)
		return nil, fmt.Errorf("failed to initialize k8s clients: %w", err)
	}
	return &RafPlugin{
		appGroup:      NewGroup(),
		clientset:     clientset,
		dynamicClient: dynamicClient,
	}, nil
}

// detectGroupMembership checks if the pod belongs to a group (e.g., Deployment, StatefulSet)
// by inspecting the owner references or annotations.
func (p *RafPlugin) detectGroupMembership(pod *corev1.Pod) string {
	if len(pod.OwnerReferences) > 0 {
		// Assuming all Pods belonging to the same Deployment/StatefulSet have the same owner UID
		return string(pod.OwnerReferences[0].UID)
	}
	// Alternatively, check for custom annotations that indicate group membership
	return pod.Annotations[AppGroupLabel]
}

type StringState string

func (s StringState) Clone() framework.StateData { return s }
