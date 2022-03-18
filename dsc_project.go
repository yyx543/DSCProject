package main

import {
	"fmt"
}

var allVirtualNodes []*virtualNode // global list of virtual nodes

func main () {
	var numOfVirtualNodes int = 5 // assume 5 virtual nodes
	var numOfRoomIds int = 10 // assume 10 different rooms
	var numOfReplicas int = 1 // assume no replication
	allVirtualNodes = make([]*virtualNode, numOfVirtualNodes)

	// room id (0 - 9)
	// roomId // numOfVirtualNodes -> [0, 4]

	// create virtual nodes 
	for id := 0; id < numOfVirtualNodes; id++ {
		nodeNew := virtualNode{nodeList: make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas), 
			hashID: id, mapping: make(map[int]int)}
		// create list of physical nodes
		for j := 0; j < numOfRoomIds/numOfVirtualNodes; j++ {
			newPhysicalNode := physicalNode{roomID: id + j*numOfVirtualNodes, studentID: 0}
			nodeNew.nodeList[j] = newPhysicalNode
			mapping[id + j*numOfVirtualNodes] = j
		}

		go nodeNew.virtualNodeWait()
		allVirtualNodes[id] = nodeNew
	}

	generalWait(1, "create", "1004123")
}

func generalWait(roomId int, op string, studentId int) { // op can be "create" or "delete"
	// hash room id
	inputHashId := roomId%numOfVirtualNodes
	
	// check which virtual node this hashed value belong to
	for _, vnode := range allVirtualNodes {
		if inputHashId == vnode.hashID {
			// create message
			msgNew := new(message)
			msgNew.msgRoomId = roomId
			msgNew.operation = op
			msgNew.msgStudentId = studentId

			vnode.ch <- msgNew
		}
	}
}

type virtualNode struct {
	ch chan message
	nodeList []physicalNode
	hashID int
	roomToPos map[int]int // mapping between physical node roomID (key) and its position in nodeList (value)
}

type message struct {
	msgRoomId int
	msgStudentId int
	operation string // "create", "delete"
}

func (vnode virtualNode) virtualNodeWait() {
	virtualNodeCh := vnode.ch
	for {
		// will take in input from general function which is the HASHED room id to be querried
		// will then identify the physical node that contains the HASHED room id
		select {
		case msg, ok := <-virtualNodeCh:
			if ok {
				// addPhysicalNode()
				roomPosition := vnode.roomToPos[msg.msgRoomId]
				roomPositionPhysicalNode := vnode.nodeList[roomPosition]

				if msg.operation == "create"{
					if roomPositionPhysicalNode.studentID != 0 { 
						// if room is booked - print already booked
						fmt.Println("The room has already been booked by Student ID: " + roomPositionPhysicalNode.studentID)
					} else { 
						// if room is NOT booked - allow booking
						roomPositionPhysicalNode.studentID = msg.msgStudentId
						fmt.Println("The room has been successfully booked!")
					}
				} else if msg.operation == "delete" {
					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking
						roomPositionPhysicalNode.studentID == 0
						fmt.Println("Booking deleted")
					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room not booked - No booking to delete")
					}
				}
			}
		}
	}
}

type physicalNode struct {
	roomID int
	studentID int
}

// func (c client) sendMessage(msg message) { //client struc method
// 	ch := c.server.ch
// 	c.vectorClock[c.id] += 1
// 	//vclock := c.vectorClock

// 	msg.vectorClock = c.vectorClock
// 	ch <- msg // this sends a message to server channel

// }

// func (s server) serverWAIT() {
// 	serverchannel := s.ch
// 	for {
// 		select {
// 		case i, ok := <-serverchannel:
// 			if ok {
// 				go s.serverBroadcast(i)

// 			}
// 		case <-exitChannel:
// 			fmt.Println("Server closing")
// 			return
// 		default:

// 		}
// 	}
// }
