package raf

import (
	"fmt"
	"sync"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Node string

type AppGroupNodes struct {
	Nodes []Node
}

func (n AppGroupNodes) Clone() framework.StateData {
	return AppGroupNodes{n.Nodes}
}

type AppGroup struct {
	mu     sync.RWMutex
	groups map[string]*AppGroupNodes
}

func NewGroup() *AppGroup {
	return &AppGroup{
		mu: sync.RWMutex{},
	}
}

func (g *AppGroup) addNode(group string, node Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	val, ok := g.groups[group]
	if !ok {
		return fmt.Errorf("failed to add node, group %v not found", group)
	}
	val.Nodes = append(val.Nodes, node)
	return nil
}

func (g *AppGroup) getNodes(group string) AppGroupNodes {
	g.mu.RLock()
	defer g.mu.RUnlock()

	val, ok := g.groups[group]
	if !ok {
		return AppGroupNodes{}
	}
	return *val
}
