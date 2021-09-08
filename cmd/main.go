package main

import (
	"fmt"
	"hash/crc32"
	"sync"
)

type Node struct {
	ID   uint32
	IP   string
	Next *Node
	Pre  *Node
}

func NewNode(IP string) *Node {
	return &Node{IP: IP}
}

func (n *Node) HashSum() uint32 {
	n.ID = crc32.ChecksumIEEE([]byte(n.IP))
	return n.ID
}

type Ring struct {
	Header *Node
}

func NewRing() *Ring {
	return &Ring{}
}

func (r Ring) PrintNodes() {
	header := r.Header
	if header == nil {
		return
	}
	for header.ID <= header.Next.ID {
		fmt.Printf("%d-%s->%d-%s\n", header.ID, header.IP, header.Next.ID, header.Next.IP)
		header = header.Next
	}
}

func (r *Ring) AddNode(node *Node) {
	if r.Header == nil {
		r.Header = node
		r.Header.Next = r.Header
		r.Header.Pre = r.Header
		return
	}
	header := r.Header
	if node.ID < r.Header.ID {
		preNode := r.Header.Pre
		preNode.Next = node
		node.Next = r.Header
		node.Pre = preNode
		r.Header.Pre = node
		r.Header = node
		return
	}
	for header.ID < header.Next.ID && node.ID > header.ID && node.ID > header.Next.ID {
		header = header.Next
	}
	nextNode := header.Next
	header.Next = node
	node.Pre = header
	node.Next = nextNode
	nextNode.Pre = node
}

func (r Ring) findNextNode(hash uint32) *Node {
	header := r.Header
	if header == nil {
		return nil
	}
	if hash < header.ID {
		return header
	}
	for header.ID < header.Next.ID && hash > header.Next.ID {
		header = header.Next
	}
	return header.Next
}

type HashRing struct {
	Nodes  map[uint32]*Node
	Ring   *Ring
	locker sync.RWMutex
}

func NewHashRing() *HashRing {
	return &HashRing{
		Nodes: make(map[uint32]*Node),
	}
}

func (r *HashRing) JoinMultiNodes(IPs ...string) {
	for _, IP := range IPs {
		r.JoinNode(IP)
	}
}

func (r *HashRing) JoinNode(IP string) {
	r.locker.Lock()
	defer r.locker.Unlock()
	node := NewNode(IP)
	hash := node.HashSum()
	if r.Ring == nil {
		r.Ring = NewRing()
	}
	r.Ring.AddNode(node)
	r.Nodes[hash] = node
}
func (r *HashRing) PrintNodes() {
	r.Ring.PrintNodes()
}

func (r *HashRing) GetNode(key string) *Node {
	hash := crc32.ChecksumIEEE([]byte(key))
	r.locker.RLock()
	defer r.locker.RUnlock()
	fmt.Printf("key:%s,hash:%d\n", key, hash)
	if node, ok := r.Nodes[hash]; ok {
		return node
	}
	return r.Ring.findNextNode(hash)
}

func main() {
	h := NewHashRing()
	h.JoinMultiNodes(
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
		"192.168.1.4",
		"192.168.1.5",
		"192.168.1.6")
	h.PrintNodes()
	node := h.GetNode("test.jpeg")
	fmt.Printf("NodeID:%d,NodeIP:%s\n", node.ID, node.IP)
}
