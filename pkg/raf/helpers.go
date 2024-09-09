package raf

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type mode uint8

const (
	UnknownMode   mode = 0
	UploadMode    mode = 1      // 1
	DownloadMode  mode = 1 << 1 // 2
	BandwidthMode mode = 3      // 3 (UploadMode | DownloadMode)
	LatencyMode   mode = 1 << 2 // 4
	AllMode       mode = 7      // 7 (UploadMode | DownloadMode | LatencyMode)
)

var modeMap = map[string]mode{
	"all":       AllMode,
	"bandwidth": BandwidthMode,
	"upload":    UploadMode,
	"download":  DownloadMode,
	"latency":   LatencyMode,
}

func Mode(m string) mode {
	if mode, exists := modeMap[m]; exists {
		return mode
	}
	return UnknownMode
}

func (m mode) String() string {
	switch m {
	case AllMode:
		return "all"
	case BandwidthMode:
		return "bandwidth"
	case UploadMode:
		return "upload"
	case DownloadMode:
		return "download"
	case LatencyMode:
		return "latency"
	default:
		return "unknown"
	}
}

// Check if a specific mode is enabled
func (m mode) IsEnabled(checkMode mode) bool {
	return m&checkMode != 0
}

func (m mode) Clone() framework.StateData {
	return m
}

func getKubernetesClient() (*kubernetes.Clientset, dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	return clientset, dynamicClient, nil
}

func getNodeItems(ctx context.Context, clientset *kubernetes.Clientset) ([]NodeItem, error) {
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var nodes []NodeItem
	for _, node := range nodeList.Items {
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				nodes = append(nodes, NodeItem{Name: node.Name, Address: addr.Address})
				break
			}
		}
	}
	return nodes, nil
}

type NodeItem struct {
	Address string
	Name    string
}
