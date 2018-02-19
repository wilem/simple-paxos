package main

import (
	"testing"
	"log"
)

func TestClusterConfig(t *testing.T) {
	
	c := NewClusterConfig(1)
	for i:= uint32(1); i <= 3; i++ {
		c.ServerList = append(c.ServerList, i)
		c.ProposerList = append(c.ProposerList, i)
		c.AcceptorList = append(c.AcceptorList, i)
		c.LearnerList = append(c.LearnerList, i)
	}
	
	err := c.SaveToFile("node1.cfg")
	if err != nil {
		t.Errorf("err:%s\n", err)
	}
	c1 := NewClusterConfig(0)

	err = c1.LoadFromFile("node1.cfg")
	if err != nil {
		t.Errorf("err:%s\n", err)
	}
	
	log.Printf("c1:%+v\n", c1)
}