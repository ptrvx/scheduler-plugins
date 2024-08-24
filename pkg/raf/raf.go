package raf

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "RafPlugin"

type RafPlugin struct{}

func (p *RafPlugin) Name() string {
	return Name
}

func (p *RafPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	fmt.Printf("Hello from RafPLugin! Pod: %v, Node: %v\n", pod, nodeInfo)
	return framework.NewStatus(framework.Success, "Node is valid")
}

func New(_ context.Context, _ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &RafPlugin{}, nil
}
