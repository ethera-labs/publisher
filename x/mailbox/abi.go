package mailbox

// mailboxABI is a focused subset of UniversalBridgeMailbox covering the three
// entrypoints the processor decodes or packs: readMessage, writeMessage and
// putInbox. The NewInboxKey / NewOutboxKey events are included so log-based
// tooling can bind the same ABI. Any additional methods should be appended
// here rather than pulled from a larger artifact.
const mailboxABI = `[
  {
    "type":"function",
    "name":"readMessage",
    "stateMutability":"nonpayable",
    "inputs":[
      {
        "name":"header",
        "type":"tuple",
        "internalType":"struct IUniversalBridgeMailbox.MessageHeader",
        "components":[
          {"name":"chainSrc","type":"uint256","internalType":"uint256"},
          {"name":"chainDest","type":"uint256","internalType":"uint256"},
          {"name":"sender","type":"address","internalType":"address"},
          {"name":"receiver","type":"address","internalType":"address"},
          {"name":"sessionId","type":"uint256","internalType":"uint256"},
          {"name":"label","type":"string","internalType":"string"}
        ]
      }
    ],
    "outputs":[
      {"name":"message","type":"bytes","internalType":"bytes"}
    ]
  },
  {
    "type":"function",
    "name":"writeMessage",
    "stateMutability":"nonpayable",
    "inputs":[
      {
        "name":"message",
        "type":"tuple",
        "internalType":"struct IUniversalBridgeMailbox.Message",
        "components":[
          {
            "name":"header",
            "type":"tuple",
            "internalType":"struct IUniversalBridgeMailbox.MessageHeader",
            "components":[
              {"name":"chainSrc","type":"uint256","internalType":"uint256"},
              {"name":"chainDest","type":"uint256","internalType":"uint256"},
              {"name":"sender","type":"address","internalType":"address"},
              {"name":"receiver","type":"address","internalType":"address"},
              {"name":"sessionId","type":"uint256","internalType":"uint256"},
              {"name":"label","type":"string","internalType":"string"}
            ]
          },
          {"name":"payload","type":"bytes","internalType":"bytes"}
        ]
      }
    ],
    "outputs":[]
  },
  {
    "type":"function",
    "name":"putInbox",
    "stateMutability":"nonpayable",
    "inputs":[
      {"name":"chainMessageSender","type":"uint256","internalType":"uint256"},
      {"name":"sender","type":"address","internalType":"address"},
      {"name":"receiver","type":"address","internalType":"address"},
      {"name":"sessionId","type":"uint256","internalType":"uint256"},
      {"name":"label","type":"string","internalType":"string"},
      {"name":"data","type":"bytes","internalType":"bytes"}
    ],
    "outputs":[]
  },
  {
    "type":"event",
    "name":"NewInboxKey",
    "anonymous":false,
    "inputs":[
      {"name":"index","type":"uint256","indexed":true,"internalType":"uint256"},
      {"name":"key","type":"bytes32","indexed":false,"internalType":"bytes32"}
    ]
  },
  {
    "type":"event",
    "name":"NewOutboxKey",
    "anonymous":false,
    "inputs":[
      {"name":"index","type":"uint256","indexed":true,"internalType":"uint256"},
      {"name":"key","type":"bytes32","indexed":false,"internalType":"bytes32"}
    ]
  }
]`
