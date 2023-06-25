using Microsoft.Coyote.Actors;
using Microsoft.Coyote.Specifications;

namespace PBFT {

    public enum State {
        AWAITING_REQUEST,
        PRE_PREPARE,
        PREPARE,
        COMMIT,
        REPLY,
        VIEW_CHANGE
    }

    public enum MessageType {
        CLIENT_REQUEST_MESSAGE,
        PRE_PREPARE_MESSAGE,
        PREPARE_MESSAGE,
        COMMIT_MESSAGE,
        REPLY_MESSAGE
    }

    public class SetupEvent : Event {

        public int nodes { get; set; }
        public List<string> messages { get; set; }

        public SetupEvent(int nodes, List<string> messages) {
            this.nodes = nodes;
            this.messages = messages;
        }

    }

    public class NodeSetupEvent : Event {

        public Boolean isPrimary;
        public int viewNumber;
        public ActorId clientId;
        public List<ActorId> nodesIds;

        public NodeSetupEvent(Boolean isPrimary, int viewNumber, ActorId clientId) {
            this.isPrimary = isPrimary;
            this.viewNumber = viewNumber;
            this.clientId = clientId;
        }
    }

    public class RequestEvent : Event {
        public string operation;
        public int timestamp;
        public ActorId clientId;

        public RequestEvent(string operation, int timestamp, ActorId clientId) {
            this.operation = operation;
            this.timestamp = timestamp;
            this.clientId = clientId;
        }
    }

    public class ReplyEvent : Event {
        public int viewNumber;
        public int timestamp;
        public ActorId clientId;
        public ActorId nodeId;
        public string requestedOperation;
        public string result;

        public ReplyEvent(int viewNumber, int timestamp, ActorId clientId, ActorId nodeId, string requestedOperation, string result) {
            this.viewNumber = viewNumber;
            this.timestamp = timestamp;
            this.clientId = clientId;
            this.nodeId = nodeId;
            this.requestedOperation = requestedOperation;
            this.result = result;
        }
    }

    public class MulticastRequest : Event {
        public string message;
        public int timestamp;

        public MulticastRequest(string message, int timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }
    }

    public class ClientMulticastRequestEvent : Event {

    }

    public class UpdateNodesIdsEvent : Event {
        public List<ActorId> nodesIds;

        public UpdateNodesIdsEvent(List<ActorId> nodesIds) {
            this.nodesIds = nodesIds;
        }
    }

    public class PrePrepareEvent : Event {
        public int viewNumber;
        public int sequenceNumber;
        public string digest;
        public RequestEvent clientRequest;

        public PrePrepareEvent(int viewNumber, int sequenceNumber, RequestEvent requestEvent, string digest) {
            this.viewNumber = viewNumber;
            this.sequenceNumber = sequenceNumber;
            this.digest = digest;
            this.clientRequest = requestEvent;
        }

    }

    public class PrepareEvent : Event {
        public int viewNumber;
        public int sequenceNumber;
        public string digest;
        public ActorId prepareSender;

        public PrepareEvent(int viewNumber, int sequenceNumber, string digest, ActorId prepareSender) {
            this.viewNumber = viewNumber;
            this.sequenceNumber = sequenceNumber;
            this.digest = digest;
            this.prepareSender = prepareSender;
        }
    }

    public class CommitEvent : Event {
        public int viewNumber;
        public int sequenceNumber;
        public string digest;
        public ActorId actorId;

        public CommitEvent(int viewNumber, int sequenceNumber, string digest, ActorId actorId) {
            this.viewNumber = viewNumber;
            this.sequenceNumber = sequenceNumber;
            this.digest = digest;
            this.actorId = actorId;
        }
    }

    public class PBFT_main {

        static void Main(string[] args) {
            IActorRuntime runtime = RuntimeFactory.Create();
            Execute(runtime);
        }

        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task<object> Execute(IActorRuntime runtime) {
            int nodes = 4;

            List<string> messages = new List<string>() { "testMessage1" };
            
            ActorId clientId = runtime.CreateActor(typeof(Client), new SetupEvent(nodes, messages));
            ActorExecutionStatus clientExecutionStatus = runtime.GetActorExecutionStatus(clientId);
            //Console.WriteLine(clientExecutionStatus);
            while (runtime.GetActorExecutionStatus(clientId).Equals(ActorExecutionStatus.Active)) {
                if (runtime.GetActorExecutionStatus(clientId).Equals(ActorExecutionStatus.None)) break;
                Thread.Sleep(1000);
            }
            Specification.Assert(true, "test");
            return null;
        }

    }

    [OnEventDoAction(typeof(ReplyEvent), nameof(HandleReplyMessage))]
    [OnEventDoAction(typeof(ClientMulticastRequestEvent), nameof(HandleMulticastRequest))]
    public class Client : Actor {
        public ActorId primaryId;
        public List<ActorId> nodesIds;
        public List<string> messages;
        public int nodes;
        public int current_view;
        public Dictionary<string, List<ReplyEvent>> repliesFromRequests;
        public Dictionary<string, string> resultOfRequest;
        private System.Timers.Timer timer = new System.Timers.Timer(1000);
        public int timeElapsed;
        public RequestEvent lastRequest;

        protected override Task OnInitializeAsync(Event initialEvent) {
            LogResult("-----");
            LogResult("1");
            this.nodes = ((SetupEvent)initialEvent).nodes;
            this.messages = ((SetupEvent)initialEvent).messages;
            this.nodesIds = new List<ActorId>();
            this.current_view = 0;
            this.repliesFromRequests = new Dictionary<string, List<ReplyEvent>>();
            this.resultOfRequest = new Dictionary<string, string>();
            this.timeElapsed = 0;

            // Create nodes 
            for (int i = 0; i < this.nodes; i++) {
                ActorId newNode;
                if (i == 0) {
                    newNode = this.CreateActor(typeof(Node), new NodeSetupEvent(true, 0, this.Id));
                    this.primaryId = newNode;
                } else {
                    newNode = this.CreateActor(typeof(Node), new NodeSetupEvent(false, 0, this.Id));
                }
                nodesIds.Add(newNode);
            }
            // send event to update nodeId list of each node
            for (int i = 0; i < this.nodes; i++) {
                this.SendEvent(nodesIds[i], new UpdateNodesIdsEvent(this.nodesIds));
            }

            // begin with sending the requests to the primary
            for(int i = 0; i < this.messages.Count(); i++) {
                RequestEvent currentRequest = new RequestEvent(this.messages[i], i, this.Id);
                this.repliesFromRequests.Add(currentRequest.operation, new List<ReplyEvent>());
                this.SendEvent(this.primaryId, currentRequest);
            }

            return base.OnInitializeAsync(initialEvent);
        }


        public void HandleMulticastRequest(Event e) {
            for (int i = 0; i < nodesIds.Count; i++) {
                this.SendEvent(nodesIds[i], this.lastRequest);
            }
        }

        public void LogResult(String toBeLogged) {
            try {
                using (StreamWriter w = File.AppendText("results/OneRequest/Bug2/F-PCT-10-5.txt")) {
                    try {
                        w.WriteLine(toBeLogged);
                    } catch (Exception ex) {
                    }
                }
            } catch (Exception exc) {
            }
        }

        public void HandleReplyMessage(Event e) {
            ReplyEvent replyEvent = (ReplyEvent)e;
            this.repliesFromRequests[replyEvent.requestedOperation].Add(replyEvent);
            if (!this.resultOfRequest.ContainsKey(replyEvent.requestedOperation)) {
                var responses = this.repliesFromRequests[replyEvent.requestedOperation]
                                .Where(x => x.requestedOperation == replyEvent.requestedOperation)
                                .GroupBy(x => new { x.result, x.timestamp });

                foreach (var group in responses) {

                    int f = (int)Math.Floor(((double)this.nodes - 1) / 3);
                    if (group.Count() > f) {
                        resultOfRequest.Add(replyEvent.requestedOperation, replyEvent.result);
                        LogResult("Client accepts the reply for operation " + replyEvent.requestedOperation + ". The result is: " + replyEvent.result);
                        if (replyEvent.result.Contains("Faulty")) {
                            this.RaiseHaltEvent();
                        } else if(resultOfRequest.Count() == this.messages.Count()) {
                            this.RaiseHaltEvent();
                        }
                    }
                }
            } else {
                // Console.WriteLine("The client has already accepted result for " + replyEvent.requestedOperation);
            }
        }
    }

    [OnEventDoAction(typeof(RequestEvent), nameof(HandleRequest))]
    [OnEventDoAction(typeof(UpdateNodesIdsEvent), nameof(HandleUpdateNodesIds))]
    [OnEventDoAction(typeof(PrePrepareEvent), nameof(HandlePrePrepare))]
    [OnEventDoAction(typeof(PrepareEvent), nameof(HandlePrepare))]
    [OnEventDoAction(typeof(CommitEvent), nameof(HandleCommit))]
    public class Node : Actor {
        public const int h = -1;
        public const int H = 100;
        public Boolean isPrimary;
        public int viewNumber;
        public int timestamp;
        public int sequenceNumber;
        public ActorId clientId;
        public List<ActorId> nodeIds;
        public Dictionary<string, string> processedRequests;
        public List<AcceptedMessage> acceptedMessages;

        public State state;
        public Dictionary<String, State> digestToState;
        public Dictionary<String, Boolean> isPrepared;

        protected override Task OnInitializeAsync(Event initialEvent) {
            this.isPrimary = ((NodeSetupEvent)initialEvent).isPrimary;
            this.viewNumber = ((NodeSetupEvent)initialEvent).viewNumber;
            this.clientId = ((NodeSetupEvent)initialEvent).clientId;
            this.acceptedMessages = new List<AcceptedMessage>();
            this.timestamp = 0;
            this.sequenceNumber = 0;
            this.state = State.AWAITING_REQUEST;
            this.processedRequests = new Dictionary<string, string>();
            this.digestToState = new Dictionary<string, State>();
            this.isPrepared = new Dictionary<string, Boolean>();
            return base.OnInitializeAsync(initialEvent);
        }

        private void HandleRequest(Event e) {
            if (isPrimary) {
                string digest = digestOfClientRequest(e);
                this.digestToState.Add(digest, State.PRE_PREPARE);
                this.isPrepared.Add(digest, false);
                PrePrepareEvent prePrepareEvent = new PrePrepareEvent(this.viewNumber, this.sequenceNumber, (RequestEvent)e, digest);
                for (int i = 0; i < this.nodeIds.Count; i++) {
                    if (this.nodeIds[i] != this.Id) {
                        this.SendEvent(this.nodeIds[i], prePrepareEvent);
                    } else {
                        this.acceptedMessages.Add(new AcceptedMessage(MessageType.CLIENT_REQUEST_MESSAGE, this.viewNumber, this.sequenceNumber, digest));
                        this.acceptedMessages.Add(new AcceptedMessage(MessageType.PRE_PREPARE_MESSAGE, this.viewNumber, this.sequenceNumber, digest));
                        this.acceptedMessages.Add(new AcceptedMessage(MessageType.PREPARE_MESSAGE, this.viewNumber, this.sequenceNumber, digest));
                        this.digestToState[digest] = State.PREPARE;
                    }
                }
                this.sequenceNumber += 1;
            }
        }

        private void HandlePrePrepare(Event e) {
            PrePrepareEvent prePrepareEvent = (PrePrepareEvent)e;
            if (prePrepareEvent.digest == digestOfClientRequest(prePrepareEvent.clientRequest)
                && this.viewNumber == ((PrePrepareEvent)e).viewNumber && prePrepareEvent.sequenceNumber > h
                && prePrepareEvent.sequenceNumber < H) {
                // if it has not accepted a message with the same v and n but different d
                int differentPrePrepareMessages = this.acceptedMessages
                                .Where(message => message.messageType == MessageType.PRE_PREPARE_MESSAGE)
                                .Where(message => message.viewNumber == this.viewNumber)
                                .Where(message => message.sequenceNumber == prePrepareEvent.sequenceNumber)
                                .Where(message => message.digest != prePrepareEvent.digest)
                                .Count();

                if (differentPrePrepareMessages == 0) {
                    this.digestToState.Add(prePrepareEvent.digest, State.PREPARE);
                    this.isPrepared.Add(prePrepareEvent.digest, false);
                    for (int i = 0; i < nodeIds.Count; i++) {
                        if (this.Id != nodeIds[i]) {
                            PrepareEvent prepareEvent = new PrepareEvent(this.viewNumber, prePrepareEvent.sequenceNumber, prePrepareEvent.digest, this.Id);
                            this.SendEvent(nodeIds[i], prepareEvent);
                        }
                    }
                    this.acceptedMessages.Add(new AcceptedMessage(MessageType.CLIENT_REQUEST_MESSAGE, prePrepareEvent.clientRequest));
                    this.acceptedMessages.Add(new AcceptedMessage(MessageType.PRE_PREPARE_MESSAGE,
                        this.viewNumber, prePrepareEvent.sequenceNumber, prePrepareEvent.digest));
                    this.acceptedMessages.Add(new AcceptedMessage(MessageType.PREPARE_MESSAGE, this.viewNumber,
                    prePrepareEvent.sequenceNumber, prePrepareEvent.digest, this.Id));
                }
            }
        }

        private void HandlePrepare(Event e) {
            PrepareEvent prepareEvent = (PrepareEvent)e;
            if (
               prepareEvent.viewNumber == this.viewNumber &&
               prepareEvent.sequenceNumber > h &&
               prepareEvent.sequenceNumber < H
            ) {
                this.acceptedMessages.Add(new AcceptedMessage(MessageType.PREPARE_MESSAGE, this.viewNumber, prepareEvent.sequenceNumber, prepareEvent.digest, prepareEvent.prepareSender));

                if (this.isPreparedChecker(prepareEvent.digest) == true && this.digestToState.ContainsKey(prepareEvent.digest)) {
                    this.isPrepared[prepareEvent.digest] = true;
                    if (this.digestToState[prepareEvent.digest] == State.PREPARE) {
                        this.digestToState[prepareEvent.digest] = State.COMMIT;
                        // Console.WriteLine("Node: " + this.Id + " is ready to commit for " + prepareEvent.digest);
                        for (int i = 0; i < nodeIds.Count(); i++) {
                            if (this.nodeIds[i] != this.Id) {
                                this.SendEvent(nodeIds[i], new CommitEvent(this.viewNumber, prepareEvent.sequenceNumber, prepareEvent.digest, this.Id));
                            } else {
                                this.acceptedMessages.Add(new AcceptedMessage(MessageType.COMMIT_MESSAGE, this.viewNumber, prepareEvent.sequenceNumber, prepareEvent.digest, this.Id));
                            }
                        }
                    }
                }
            }
        }

        public void LogResult(String toBeLogged) {
            try {
                using (StreamWriter w = File.AppendText("results/OneRequest/Bug2/F-PCT-10-5.txt")) {
                    try {
                        w.WriteLine(toBeLogged);
                    } catch (Exception ex) {
                    }
                }
            } catch (Exception exc) {
            }
        }

        private void HandleCommit(Event e) {
            CommitEvent commitEvent = (CommitEvent)e;
            if (commitEvent.viewNumber == this.viewNumber &&
               commitEvent.sequenceNumber > h &&
               commitEvent.sequenceNumber < H) {
                this.acceptedMessages.Add(new AcceptedMessage(MessageType.COMMIT_MESSAGE, this.viewNumber, commitEvent.sequenceNumber, commitEvent.digest, this.Id));

                // commited local is true if prepared is true and 2f+1 commits that match the pre-prepare for m
                // they match if they have the same view_number, sequence_number, digest
                if (!this.isPrepared.ContainsKey(commitEvent.digest)) {
                    //Console.WriteLine("Problem");
                    // this.RaiseHaltEvent();
                    // LogResult("Problem");
                } else if (this.isPrepared[commitEvent.digest] == true && this.digestToState[commitEvent.digest] != State.REPLY) {
                    // izvadi pre-prepare message-a 
                    IEnumerable<AcceptedMessage> potentialPrePrepareMessage = this.acceptedMessages
                                    .Where(x => x.messageType.Equals(MessageType.PRE_PREPARE_MESSAGE))
                                    .Where(x => x.digest == commitEvent.digest);

                    if (potentialPrePrepareMessage.Count() == 1) {
                        AcceptedMessage prePrepareMessage = potentialPrePrepareMessage.First();

                        IEnumerable<AcceptedMessage> commitMessages = this.acceptedMessages
                                    .Where(x => x.digest == prePrepareMessage.digest)
                                    .Where(x => x.sequenceNumber == prePrepareMessage.sequenceNumber)
                                    .Where(x => x.viewNumber == prePrepareMessage.viewNumber);

                        int f = (int)Math.Floor(((double)this.nodeIds.Count() - 1) / 3);

                        if (commitMessages.Count() >= 2 * f + 1) {
                            this.digestToState[commitEvent.digest] = State.REPLY;
                            // Console.WriteLine("Node " + this.Id + " can send reply to the client for " + commitEvent.digest);
                            string operationResult = performOperation(commitEvent.digest);
                            this.processedRequests.Add(commitEvent.digest, operationResult);
                            ReplyEvent replyEvent = new ReplyEvent(this.viewNumber, 0, this.clientId, this.Id, commitEvent.digest, operationResult);
                            this.SendEvent(this.clientId, replyEvent);
                        }
                    }
                }
            }
        }

        private Boolean isPreparedChecker(string digest) {
            IEnumerable<AcceptedMessage> clientRequest = this.acceptedMessages
                                    .Where(x => x.messageType.Equals(MessageType.CLIENT_REQUEST_MESSAGE))
                                    .Where(clientRequest => clientRequest.digest == digest);

            if (clientRequest.Count() > 0) {
                IEnumerable<AcceptedMessage> prePrepareMessages = this.acceptedMessages
                                    .Where(x => x.messageType.Equals(MessageType.PRE_PREPARE_MESSAGE))
                                    .Where(x => x.digest == digest)
                                    .Where(x => x.sequenceNumber > h && x.sequenceNumber < H)
                                    .Where(x => x.viewNumber == this.viewNumber);

                if (prePrepareMessages.Count() == 1) {

                    AcceptedMessage prePrepareMessage = prePrepareMessages.First();

                    IEnumerable<AcceptedMessage> prepareMessages = this.acceptedMessages
                                    .Where(x => x.messageType.Equals(MessageType.PREPARE_MESSAGE))
                                    .Where(x => x.viewNumber == prePrepareMessage.viewNumber)
                                    .Where(x => x.sequenceNumber == prePrepareMessage.sequenceNumber)
                                    .Where(x => x.digest == prePrepareMessage.digest)
                                    .DistinctBy(x => x.senderId);

                    int f = (int)Math.Floor(((double)this.nodeIds.Count() - 1) / 3);
                    if (prepareMessages.Count() >= 2 * f) {
                        return true;
                    } else {
                        // Console.WriteLine("Node " + this.Id + " does not have enough prepare messages for " + digest + " . It has " + prepareMessages.Count());
                        return false;
                    }
                } else {
                    // Console.WriteLine("Node " + this.Id + " does not have PrePrepare message for " + digest);
                }
            } else {
                // Console.WriteLine("Node " + this.Id + " does not have Client request for " + digest);
                return false;
            }

            return false;
        }

        private void HandleUpdateNodesIds(Event e) {
            this.nodeIds = ((UpdateNodesIdsEvent)e).nodesIds;
        }

        public string performOperation(string operation) {
            if(this.Id.Value == 3) {
                return operation+" Faulty";
            }
            return operation;
        }

        public string digestOfClientRequest(Event e) {
            RequestEvent requestEvent = (RequestEvent)e;
            return requestEvent.operation;
        }
    }


    public class AcceptedMessage {

        public MessageType messageType;
        public int viewNumber;
        public int sequenceNumber;
        public string digest;
        public ActorId senderId;
        public RequestEvent clientRequest;

        public AcceptedMessage(MessageType messageType, int viewNumber, int sequenceNumber, string digest) {
            this.messageType = messageType;
            this.viewNumber = viewNumber;
            this.sequenceNumber = sequenceNumber;
            this.digest = digest;
        }

        public AcceptedMessage(MessageType messageType, int viewNumber, int sequenceNumber, string digest, ActorId senderId) {
            this.messageType = messageType;
            this.viewNumber = viewNumber;
            this.sequenceNumber = sequenceNumber;
            this.digest = digest;
            this.senderId = senderId;
        }

        public AcceptedMessage(MessageType messageType, RequestEvent clientRequest) {
            this.messageType = messageType;
            this.clientRequest = clientRequest;
            this.digest = clientRequest.operation;
        }

        public string toString() {
            return this.messageType.ToString();
        }
    }
}

