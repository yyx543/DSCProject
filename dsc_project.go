package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	numOfVirtualNodes int = 5 // assume 5 virtual nodes
	numOfReplicas     int = 3 // assume no replication
	rReplication      int = 3
	wReplication      int = 4
	numOfBackUps      int = 10
)

var numOfRoomIds int = 10          // assume 10 different rooms
var allVirtualNodes []*virtualNode // global list of virtual nodes
var backUpNodes []*physicalNode    // storage of backup physical nodes

func main() {

	// room id (0 - 9)
	// roomId // numOfVirtualNodes -> [0, 4]

	// scalability
	// use slicing
	// append to nodeList
	// update roomToPos

	allVirtualNodes = make([]*virtualNode, numOfVirtualNodes)
	backUpNodes = make([]*physicalNode, numOfBackUps)

	//populating backUpNodes
	for id := 0; id < numOfBackUps; id++ {
		nodeBackUpNew := physicalNode{roomID: numOfRoomIds, studentID: 0, localLogicalClock: 0, parentVnode: nil, isBackUp: true, ch: make(chan message)}
		backUpNodes[id] = &nodeBackUpNew
		go nodeBackUpNew.physicalNodeWait()
	}

	// create virtual nodes
	vMutex := sync.RWMutex{}
	pMutex := sync.RWMutex{}
	for id := 0; id < numOfVirtualNodes; id++ {
		nodeNew := virtualNode{ch: make(chan message),
			nodeList:     make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas),
			hashID:       id,
			roomToPos:    make(map[int]int),
			replyCount:   make(map[string]int),
			vnodeMutex:   &vMutex,
			pnodeReplies: make(map[string]*physicalNode),
			pnodeMutex:   &pMutex}

		for j := 0; j < numOfRoomIds/numOfVirtualNodes; j++ {
			for k := 0; k < numOfReplicas; k++ {
				var newPhysicalNode physicalNode
				if k == 0 {
					// coordinator pnode
					newPhysicalNode = physicalNode{roomID: (id + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0, localLogicalClock: 0, parentVnode: &nodeNew, isBackUp: false, ch: make(chan message)}

					nodeNew.roomToPos[(id+j*numOfVirtualNodes)%numOfRoomIds] = j
				} else {
					// replica pnodes
					newPhysicalNode = physicalNode{roomID: (id + k + (numOfVirtualNodes - numOfReplicas) + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0, localLogicalClock: 0, parentVnode: &nodeNew, isBackUp: false, ch: make(chan message)} //localVectorClock: make([]int, numOfRoomIds

					nodeNew.roomToPos[(id+k+(numOfVirtualNodes-numOfReplicas)+j*numOfVirtualNodes)%numOfRoomIds] = j + k*numOfRoomIds/numOfVirtualNodes
				}
				nodeNew.nodeList[j+k*numOfRoomIds/numOfVirtualNodes] = &newPhysicalNode
				go newPhysicalNode.physicalNodeWait()

			}
			//nodeNew.roomToPos[id+j*numOfVirtualNodes] = j

		}
		// fmt.Println(nodeNew.roomToPos)
		go nodeNew.virtualNodeWait()
		allVirtualNodes[id] = &nodeNew
	}
	// checkPhysicalNodes()

	// this to test multiple nodes

	// Scenario 1 (Consistency) - verification with read + 2 users booking same room + verification with read
	// go generalWait(1, "read", 1004567)
	// time.Sleep(1 * time.Second)
	// go generalWait(1, "create", 1004999)
	// go generalWait(1, "create", 1004123)
	// time.Sleep(5 * time.Second)
	// go generalWait(1, "read", 1004567)

	// Scenario 2 (Consistency) - attempt to delete booking when (1) is not yours (2) is yours (3) is empty
	// go generalWait(1, "create", 1004999)
	// time.Sleep(5 * time.Second)
	// go generalWait(1, "delete", 1004123) // not yours
	// time.Sleep(5 * time.Second)
	// go generalWait(1, "delete", 1004999) // yours
	// time.Sleep(5 * time.Second)
	// go generalWait(1, "delete", 1004999) // empty

	// Scenario 3 (Scalability) - add new room (pnode)
	// addNewRoom()
	// time.Sleep(1 * time.Second)
	// go generalWait(10, "create", 1004123)

	// Scenario 4 (Fault Tolerance) - simulate pnode dying
	// go allVirtualNodes[1].nodeList[allVirtualNodes[1].roomToPos[1]].die(30)
	// go allVirtualNodes[3].nodeList[allVirtualNodes[3].roomToPos[1]].die(30)
	// time.Sleep(5 * time.Second)
	// go generalWait(1, "create", 1004999)

	//this takes in input from client from command line
	var inputStudentID string
	var inputRoomNumber string
	var inputOperation string
	var validRoomNumber int
	var validStudentNumber int

	for {
		// Scenario 5 - Manual input (UI)

		for {
			fmt.Println("What is your student ID")
			fmt.Scanln(&inputStudentID)

			if i, err := strconv.Atoi(inputStudentID); err == nil {
				validStudentNumber = i
				break

			} else {
				fmt.Println("Please input a valid student number")
			}
		}

		for {
			fmt.Println("Do you want to create, delete or read a booking?\n create \t delete \t read")
			fmt.Scanln(&inputOperation)
			if inputOperation == "create" || inputOperation == "delete" || inputOperation == "read" {
				break
			} else {
				fmt.Println("Incorrect input, please choose one of the options")
			}
		}

		// exitCheck:
		for {
			fmt.Println("Enter the room number from 0-9")
			fmt.Scanln(&inputRoomNumber)

			// if inputRoomNumber.(T) == int {

			// }
			// if inputRoomNumber >= 0 && inputRoomNumber < numOfRoomIds {
			// 	break
			// } else {
			// 	fmt.Println("Invalid input, please enter room number from 0-9 again")
			// }
			if i, err := strconv.Atoi(inputRoomNumber); err == nil {
				if i >= 0 && i < 10 {
					validRoomNumber = i
					break
				} else {
					fmt.Println("Please input a number from 0 to 9")
				}

			} else {
				fmt.Println("Please input a valid number")
			}
		}

		go generalWait(validRoomNumber, inputOperation, validStudentNumber)
		time.Sleep(5 * time.Second)

	}
}

func checkPhysicalNodes() { // for testing purposes
	for _, vnode := range allVirtualNodes {
		fmt.Println("Virtual Node is ", vnode.hashID)
		for _, pnode := range vnode.nodeList {
			fmt.Println("Physical Node is ", pnode.roomID)
		}
		fmt.Println("Room to pos")
		fmt.Println(vnode.roomToPos)

	}

}

func generalWait(roomId int, op string, studentId int) { // op can be "create" or "delete"
	// hash room id
	inputHashId := roomId % numOfVirtualNodes
	//fmt.Println("Entering General wait")
	fmt.Println("Trying to " + op + " room " + strconv.Itoa(roomId) + " by studentID: " + strconv.Itoa(studentId))

	// check which virtual node this hashed value belong to
	for _, vnode := range allVirtualNodes {
		if inputHashId == vnode.hashID {
			// create message
			msgNew := new(message)
			msgNew.msgRoomId = roomId
			msgNew.operation = op
			msgNew.msgStudentId = studentId

			vnodech := vnode.ch

			vnodech <- *msgNew
		}
	}

}

type virtualNode struct {
	ch        chan message
	nodeList  []*physicalNode
	hashID    int
	roomToPos map[int]int // mapping between physical node roomID (key) and its position in nodeList (value)
	// replyCount  map[int]map[int]int // replyCount[roomID][studentID] to get reply count
	replyCount  map[string]int // map[roomID + studentID] to get count
	requestDONE bool

	replicaAliveArr []*virtualNode // create array for replicas

	vnodeMutex   *sync.RWMutex
	pnodeReplies map[string]*physicalNode // map[roomID + studentID] to get pnode replies
	pnodeMutex   *sync.RWMutex

	// requestBoolean bool
}

type message struct {
	msgRoomId    int
	msgStudentId int
	operation    string // "create", "delete"
	sender       *virtualNode
	senderPnode  *physicalNode
}
type physicalNode struct {
	roomID            int
	studentID         int
	localLogicalClock int
	originalPnode     *physicalNode
	ch                chan message
	parentVnode       *virtualNode
	isBackUp          bool
	isDead            bool
}

func (vnode *virtualNode) virtualNodeWait() {
	//fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " is now waiting")
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

				if msg.operation == "create" || msg.operation == "delete" || msg.operation == "read" {
					go vnode.requestReplicate(msg.msgRoomId, msg.msgStudentId, msg.operation)

				} else if msg.operation == "request" {
					// handle reqests for replies

					// physicalnodereply
					go vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].physicalNodeReply(msg)

				} else if msg.operation == "reply" {

					// receive replies to add to replycount
					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)

					vnode.pnodeMutex.Lock()

					if vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].localLogicalClock < msg.senderPnode.localLogicalClock {
						vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].localLogicalClock = msg.senderPnode.localLogicalClock
						vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].studentID = msg.senderPnode.studentID
					}

					vnode.pnodeMutex.Unlock()

					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] += 1
					vnode.vnodeMutex.Unlock()

				} else if msg.operation == "create-success" {

					if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked - print already booked
						fmt.Println("The room has already been booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					} else {
						// if room is NOT booked - allow booking
						// update create operation here locally
						go vnode.requestReplicaOverwrite(msg.msgRoomId, msg.msgStudentId, msg.operation)

					}
				} else if msg.operation == "delete-success" {

					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking
						// update delete operation here locally
						go vnode.requestReplicaOverwrite(msg.msgRoomId, msg.msgStudentId, msg.operation)
					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room " + strconv.Itoa(msg.msgRoomId) + " not booked - No booking to delete")
					}

				} else if msg.operation == "read-success" {
					if roomPositionPhysicalNode.studentID == 0 {
						fmt.Println("Room " + strconv.Itoa(roomPositionPhysicalNode.roomID) + " not booked")
					} else {
						fmt.Println("Room " + strconv.Itoa(roomPositionPhysicalNode.roomID) + " is booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					}

				} else if msg.operation == "create-update" {

					if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked - print already booked
						fmt.Println("The room has already been booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					} else {
						// if room is NOT booked - allow booking
						// update create operation here locally

						fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " updating replica of roomID " + strconv.Itoa(roomPositionPhysicalNode.roomID))
						roomPositionPhysicalNode.studentID = msg.msgStudentId
						roomPositionPhysicalNode.localLogicalClock = msg.senderPnode.localLogicalClock

						go vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].physicalNodeReply(msg)
					}

				} else if msg.operation == "delete-update" {

					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking
						// update delete operation here locally
						fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " updating replica of roomID " + strconv.Itoa(roomPositionPhysicalNode.roomID))
						roomPositionPhysicalNode.studentID = 0
						roomPositionPhysicalNode.localLogicalClock = msg.senderPnode.localLogicalClock

						go vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].physicalNodeReply(msg)

					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room " + strconv.Itoa(msg.msgRoomId) + " not booked - No booking to delete")
					}

				} else if msg.operation == "create-overwrite" {

					fmt.Println("The room " + strconv.Itoa(msg.msgRoomId) + " has been successfully booked by " + strconv.Itoa(msg.msgStudentId))

					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)
					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] = 0
					vnode.vnodeMutex.Unlock()

				} else if msg.operation == "delete-overwrite" {

					fmt.Println("RoomID " + strconv.Itoa(msg.msgRoomId) + " booking has been deleted by " + strconv.Itoa(msg.msgStudentId))

					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)
					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] = 0
					vnode.vnodeMutex.Unlock()

				} else if msg.operation == "reply from pnode" {
					var temp []*virtualNode = nil
					for _, replica := range vnode.replicaAliveArr {
						if msg.sender != replica {
							temp = append(temp, replica)
						}
					}
					vnode.replicaAliveArr = temp
				}
			}
		}
	}
}

func (pnode *physicalNode) physicalNodeReply(msg message) {
	if !pnode.isDead {
		fmt.Println("PNode " + strconv.Itoa(pnode.roomID) + " of vnode " + strconv.Itoa(pnode.parentVnode.hashID) + " is replying to replication request")
		senderCH := msg.sender.ch

		newMSG := new(message)
		newMSG.operation = "reply"
		newMSG.msgRoomId = msg.msgRoomId
		newMSG.msgStudentId = msg.msgStudentId
		newMSG.senderPnode = pnode
		senderCH <- *newMSG
	}

}

func (vnode *virtualNode) requestReplicate(roomID int, studentID int, op string) {
	// this is the go routine that will busy wait for replies
	fmt.Println("Requesting replica reponse")

	// initialise replyCount
	key := strconv.Itoa(roomID) + strconv.Itoa(studentID)
	vnode.vnodeMutex.Lock()
	vnode.replyCount[key] = 0
	vnode.vnodeMutex.Unlock()

	// initialise pnodeReplies
	vnode.pnodeMutex.Lock()
	vnode.pnodeReplies[key] = vnode.nodeList[vnode.roomToPos[roomID]]
	vnode.pnodeMutex.Unlock()

	newMSG := new(message)
	newMSG.operation = "request"
	newMSG.msgRoomId = roomID
	newMSG.msgStudentId = studentID
	newMSG.sender = vnode

	for i := 0; i < numOfReplicas; i++ {
		next := (vnode.hashID + i) % numOfVirtualNodes
		nextCH := allVirtualNodes[next].ch
		nextCH <- *newMSG
	}

	// wait for replies
	start := time.Now()
	for {
		// if majority, break (SUCCESS)

		if op == "create" || op == "delete" {
			// for write operation
			vnode.vnodeMutex.Lock()
			if vnode.replyCount[key] >= wReplication-1 {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = op + "-success"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		} else if op == "read" {
			// for read operation
			vnode.vnodeMutex.Lock()
			if vnode.replyCount[key] >= rReplication {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = op + "-success"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		}

		// if timeout, replication failed (FAILURE)
		elapsed := time.Since(start)
		if elapsed >= time.Second*5 {
			// did not receive response from at least one replica
			fmt.Println("At least one replica has died..")
			// check if other two replicas are alive

			for i := 0; i < numOfReplicas; i++ {
				replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
				replicaPnode := replicaVnode.nodeList[replicaVnode.roomToPos[roomID]]
				// fmt.Println("pnode " + strconv.Itoa(replicaPnode.roomID) + ", vnode " + strconv.Itoa((vnode.hashID+i)%numOfVirtualNodes))
				newMSG := new(message)
				newMSG.operation = "check alive"
				newMSG.msgStudentId = studentID
				newMSG.msgRoomId = roomID
				newMSG.sender = vnode
				newMSG.senderPnode = vnode.nodeList[vnode.roomToPos[roomID]]
				replicaPnode.ch <- *newMSG // check if replica is dead or alive
				vnode.replicaAliveArr = append(vnode.replicaAliveArr, replicaVnode)
			}

			start := time.Now()
			for {
				if len(vnode.replicaAliveArr) == 0 {
					// fmt.Println("Replicas are alive - 1")
					if op == "create" || op == "delete" {
						// for write operation
						// send message back to virtualNodeWait() - successful replication
						successMSG := new(message)
						successMSG.operation = op + "-success"
						successMSG.msgRoomId = roomID
						successMSG.msgStudentId = studentID
						successMSG.sender = vnode
						vnode.ch <- *successMSG
						return
					} else if op == "read" {
						// for read operation
						// send message back to virtualNodeWait() - successful replication
						successMSG := new(message)
						successMSG.operation = op + "-success"
						successMSG.msgRoomId = roomID
						successMSG.msgStudentId = studentID
						successMSG.sender = vnode
						vnode.ch <- *successMSG
					}
					return
				}
				elapsed2 := time.Since(start)
				if elapsed2 >= time.Second*5 {
					// check if receive replies
					for _, replica := range vnode.replicaAliveArr {
						fmt.Println("Replica " + strconv.Itoa(replica.hashID) + " is dead")
						pReplicaForInfo := getNextAlivePnode(roomID, roomID%numOfVirtualNodes, replica.hashID)
						pBackUpNode := findBackup()

						// update backupNode
						pBackUpNode.roomID = pReplicaForInfo.roomID
						pBackUpNode.studentID = pReplicaForInfo.studentID
						pBackUpNode.localLogicalClock = pReplicaForInfo.localLogicalClock
						pBackUpNode.parentVnode = replica
						deadNode := replica.nodeList[replica.roomToPos[roomID]]
						pBackUpNode.originalPnode = deadNode

						// change pointer of nodelist
						replica.nodeList[replica.roomToPos[roomID]] = pBackUpNode
						// transfer replica data to new physical node
						// vnode.createNewReplicaData(replica.hashID, roomID)
						msg := new(message)
						pBackUpNode.ch <- *msg
					}

					vnode.replicaAliveArr = nil

					time.Sleep(2 * time.Second)
					fmt.Println("completed replication, starting client request again")
					go generalWait(roomID, op, studentID)
					return
				}
			}
		}
	}
}

func (vnode *virtualNode) requestReplicaOverwrite(roomID int, studentID int, op string) {

	key := strconv.Itoa(roomID) + strconv.Itoa(studentID)
	vnode.vnodeMutex.Lock()
	vnode.replyCount[key] = 0
	vnode.vnodeMutex.Unlock()

	vnode.pnodeMutex.Lock()
	vnode.pnodeReplies[key] = vnode.nodeList[vnode.roomToPos[roomID]]
	vnode.pnodeMutex.Unlock()

	vnode.nodeList[vnode.roomToPos[roomID]].localLogicalClock += 1

	// request other vnodes to replicate data
	fmt.Println("Requesting replica overwrite")
	if op == "create-success" {
		updateMSG := new(message)
		updateMSG.msgRoomId = roomID
		updateMSG.msgStudentId = studentID
		updateMSG.operation = "create-update"
		updateMSG.sender = vnode
		updateMSG.senderPnode = vnode.nodeList[vnode.roomToPos[roomID]] // updated pnode

		for i := 0; i < numOfReplicas; i++ {
			replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
			replicaVnode.ch <- *updateMSG
		}
	} else if op == "delete-success" {
		updateMSG := new(message)
		updateMSG.msgRoomId = roomID
		updateMSG.msgStudentId = studentID
		updateMSG.operation = "delete-update"
		updateMSG.sender = vnode
		updateMSG.senderPnode = vnode.nodeList[vnode.roomToPos[roomID]] // updated pnode

		for i := 0; i < numOfReplicas; i++ {
			replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
			replicaVnode.ch <- *updateMSG
		}
	}

	start := time.Now()
	for {
		if op == "create-success" {
			vnode.vnodeMutex.Lock()
			// fmt.Println(vnode.replyCount[key])
			if vnode.replyCount[key] >= wReplication-1 {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = "create-overwrite"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		} else if op == "delete-success" {
			vnode.vnodeMutex.Lock()
			if vnode.replyCount[key] >= wReplication-1 {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = "delete-overwrite"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		}

		elapsed := time.Since(start)
		if elapsed > time.Second*5 {
			// did not receive response from at least one replica
			fmt.Println("At least one replica has died....")

			// check if other two replicas are alive
			for i := 0; i < numOfReplicas; i++ {
				replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
				replicaPnode := replicaVnode.nodeList[replicaVnode.roomToPos[roomID]]
				// fmt.Println("pnode " + strconv.Itoa(replicaPnode.roomID) + ", vnode " + strconv.Itoa((vnode.hashID+i)%numOfVirtualNodes))
				newMSG := new(message)
				newMSG.operation = "check alive"
				newMSG.msgStudentId = studentID
				newMSG.msgRoomId = roomID
				newMSG.sender = vnode
				newMSG.senderPnode = vnode.nodeList[vnode.roomToPos[roomID]]
				replicaPnode.ch <- *newMSG // check if replica is dead or alive
				vnode.replicaAliveArr = append(vnode.replicaAliveArr, replicaVnode)
			}
			start := time.Now()
			for {
				if len(vnode.replicaAliveArr) == 0 {
					// fmt.Println("Replicas are alive - 2")
					// fmt.Println(op)
					if op == "create-success" {
						// send message back to virtualNodeWait() - successful replication
						successMSG := new(message)
						successMSG.operation = "create-overwrite"
						successMSG.msgRoomId = roomID
						successMSG.msgStudentId = studentID
						successMSG.sender = vnode
						vnode.ch <- *successMSG
						return
					} else if op == "delete-success" {
						// send message back to virtualNodeWait() - successful replication
						successMSG := new(message)
						successMSG.operation = "delete-overwrite"
						successMSG.msgRoomId = roomID
						successMSG.msgStudentId = studentID
						successMSG.sender = vnode
						vnode.ch <- *successMSG
						return
					}

					return
				}
				elapsed2 := time.Since(start)
				if elapsed2 >= time.Second*5 {
					// check if receive replies
					for _, replica := range vnode.replicaAliveArr {
						fmt.Println("Replica " + strconv.Itoa(replica.hashID) + " is dead")
						pReplicaForInfo := getNextAlivePnode(roomID, roomID%numOfVirtualNodes, replica.hashID)
						pBackUpNode := findBackup()

						// update backupNode
						pBackUpNode.roomID = pReplicaForInfo.roomID
						pBackUpNode.studentID = pReplicaForInfo.studentID
						pBackUpNode.localLogicalClock = pReplicaForInfo.localLogicalClock
						pBackUpNode.parentVnode = replica
						deadNode := replica.nodeList[replica.roomToPos[roomID]]
						pBackUpNode.originalPnode = deadNode

						// change pointer of nodelist
						replica.nodeList[replica.roomToPos[roomID]] = pBackUpNode

						msg := new(message)
						pBackUpNode.ch <- *msg
					}

					vnode.replicaAliveArr = nil

					time.Sleep(2 * time.Second)
					if op == "create-success" {
						go generalWait(roomID, "create", studentID)
					} else if op == "delete-success" {
						go generalWait(roomID, "delete", studentID)
					}

					return
				}
			}
		}
	}
}

func getNextAlivePnode(roomID int, mainVnodeID int, deadVnodeID int) *physicalNode {
	if mainVnodeID != deadVnodeID {
		// take from main vnode
		vnodeInQuestion := allVirtualNodes[mainVnodeID]
		return vnodeInQuestion.nodeList[vnodeInQuestion.roomToPos[roomID]]
	} else {
		// take from replica vnode
		vnodeInQuestion := allVirtualNodes[(mainVnodeID+1)%numOfVirtualNodes]
		return vnodeInQuestion.nodeList[vnodeInQuestion.roomToPos[roomID]]
	}
}

func findBackup() *physicalNode {
	//returns pointer for a physical node

	for _, backupNode := range backUpNodes {
		if backupNode.roomID == numOfRoomIds {
			fmt.Println("Backup found")
			return backupNode
		}
	}
	fmt.Println("too bad")
	return nil
}

func (pnode *physicalNode) physicalNodeWait() {
	physicalNodeCh := pnode.ch
	for {
		if !pnode.isDead {

			if pnode.isBackUp == true && pnode.roomID != numOfRoomIds {
				// check back if primary replica is alive
				originalCH := pnode.originalPnode.ch

				newMSG := new(message)
				newMSG.sender = pnode.parentVnode
				newMSG.operation = "check alive from backup"
				newMSG.senderPnode = pnode // backup pnode

				originalCH <- *newMSG
			}

			select {
			case msg, ok := <-physicalNodeCh:
				if pnode.isDead {
					fmt.Println("someone is dead...")
				} else if ok {
					if msg.operation == "check alive" {
						newMSG := new(message)
						newMSG.sender = pnode.parentVnode
						newMSG.operation = "reply from pnode"
						msg.sender.ch <- *newMSG
					} else if msg.operation == "check alive from backup" {
						// retrieve back data from replica + remove replica
						fmt.Println("primary pnode received 'check alive' message from backup pnode")
						// retrieve data
						pnode.roomID = msg.senderPnode.roomID
						pnode.studentID = msg.senderPnode.studentID
						pnode.localLogicalClock = msg.senderPnode.localLogicalClock

						// set replica boolean to false
						msg.senderPnode.roomID = numOfRoomIds
						pnode.parentVnode.nodeList[pnode.parentVnode.roomToPos[pnode.roomID]] = pnode

						// remove replica
						backupCH := msg.sender.ch

						newMSGReplace := new(message)
						newMSGReplace.operation = "reset backup"
						backupCH <- *newMSGReplace

					} else if msg.operation == "reset backup" {
						fmt.Println("reset backup pnode")
						pnode.roomID = numOfRoomIds
						pnode.studentID = 0
						pnode.localLogicalClock = 0
						pnode.parentVnode = nil
						pnode.originalPnode = nil
					}
				}
			}
		}

		// will take in input from general function which is the HASHED room id to be querried
		// will then identify the physical node that contains the HASHED room id

	}
}

func (vnode *virtualNode) replicateData(roomID int, studentID int) {
	fmt.Println("Replication request successfull, replicating data")

	// get next replicate
	for i := 1; i < numOfReplicas; i++ {
		next := (vnode.hashID + i) % numOfVirtualNodes
		physicalNode := allVirtualNodes[next].nodeList[vnode.roomToPos[roomID]]
		physicalNode.studentID = studentID
	}
	fmt.Println("Replication successful!")
}

func (pnode *physicalNode) die(t int) {
	fmt.Println("Pnode of roomID " + strconv.Itoa(pnode.roomID) + " of vnode " + strconv.Itoa(pnode.parentVnode.hashID) + " is dead")

	pnode.isDead = true
	time.Sleep(time.Duration(t) * time.Second)
	fmt.Println("Pnode of roomID " + strconv.Itoa(pnode.roomID) + " of vnode " + strconv.Itoa(pnode.parentVnode.hashID) + " came back alive...")
	pnode.isDead = false

}

func addNewRoom() {
	// create and update pnodes a vnodes
	for i := 0; i < numOfReplicas; i++ {
		pnode := physicalNode{
			roomID:            numOfRoomIds,
			studentID:         0,
			localLogicalClock: 0,
			parentVnode:       allVirtualNodes[(numOfRoomIds+i)%numOfVirtualNodes],
			ch:                make(chan message),
		}
		pnode.parentVnode.roomToPos[pnode.roomID] = len(pnode.parentVnode.nodeList)
		pnode.parentVnode.nodeList = append(pnode.parentVnode.nodeList, &pnode)
		go pnode.physicalNodeWait()

	}

	// increase num of room id
	numOfRoomIds += 1

	// increase backup room id number
	for _, pnode := range backUpNodes {
		if pnode.roomID == numOfRoomIds-1 {
			pnode.roomID = numOfRoomIds
		}
	}
	fmt.Println("new room added")
}
