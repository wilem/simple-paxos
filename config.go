package main

import (
	//"log"
	"io/ioutil"
	"encoding/json"
	"os"
)

//ClusterConfig - config for cluster node.
type ClusterConfig struct {
	NodeID 		 uint32
	//S = P + A + L
	ServerList 	 []uint32
	ProposerList []uint32
	AcceptorList []uint32
	LearnerList  []uint32
}

// NewClusterConfig : 
func NewClusterConfig(id uint32) *ClusterConfig {
	c := new(ClusterConfig)
	c.NodeID = id
	return c
}

// SaveToFile : 
func (c *ClusterConfig) SaveToFile(file string) error {
	bs, err := json.Marshal(c)
	if err != nil { return err }

	err = ioutil.WriteFile(file, bs, os.FileMode(0777))
	if err != nil { return err }
	
	return nil
}

// LoadFromFile :
func (c *ClusterConfig) LoadFromFile(file string) error {
	bs, err := ioutil.ReadFile(file)
	err = json.Unmarshal(bs, c)
	if err != nil { return err }

	return nil
}