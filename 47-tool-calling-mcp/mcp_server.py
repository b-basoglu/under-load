"""A minimal MCP server over stdio (JSON-RPC 2.0, newline-delimited), implementing initialize, tools/list and tools/call.
It exposes the ticketing system's CRM as tools. Run by the client as a subprocess."""
import json
import sys

CUSTOMERS = {"CUS-10442": {"name": "Northwind", "plan": "Enterprise", "seats": 48, "open_tickets": 3},
             "CUS-22871": {"name": "Halcyon", "plan": "Pro", "seats": 120, "open_tickets": 17},
             "CUS-30915": {"name": "Brightline", "plan": "Starter", "seats": 3, "open_tickets": 0}}
TICKETS = {"T-5512": {"status": "pending", "assignee": "Sam", "priority": "high", "customer": "CUS-22871"},
           "T-5530": {"status": "solved", "assignee": "Ana", "priority": "low", "customer": "CUS-10442"},
           "T-5541": {"status": "open", "assignee": None, "priority": "urgent", "customer": "CUS-22871"}}
TOOLS = [
    {"name": "get_customer", "description": "Look up a customer account: plan, seats and open tickets.",
     "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string", "pattern": "^CUS-[0-9]{5}$"}}, "required": ["customer_id"]}},
    {"name": "get_ticket", "description": "Get a ticket's current status, assignee and priority.",
     "inputSchema": {"type": "object", "properties": {"ticket_id": {"type": "string", "pattern": "^T-[0-9]{4}$"}}, "required": ["ticket_id"]}},
]
EXTRA = [{"name": "list_open_tickets", "description": "List open tickets for a customer.",
          "inputSchema": {"type": "object", "properties": {"customer_id": {"type": "string"}}, "required": ["customer_id"]}}]


def call(name, args):
    if name == "get_customer":
        c = CUSTOMERS.get(args.get("customer_id"))
        return {"error": "unknown customer"} if c is None else c
    if name == "get_ticket":
        t = TICKETS.get(args.get("ticket_id"))
        return {"error": "unknown ticket"} if t is None else t
    if name == "list_open_tickets":
        return [k for k, t in TICKETS.items() if t["customer"] == args.get("customer_id") and t["status"] != "solved"]
    return {"error": f"unknown tool {name}"}


def main():
    tools = TOOLS + (EXTRA if "--with-list-tool" in sys.argv else [])
    for line in sys.stdin:
        req = json.loads(line)
        m, rid = req.get("method"), req.get("id")
        if m == "initialize":
            res = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {}}, "serverInfo": {"name": "halcyon-crm", "version": "1.0"}}
        elif m == "tools/list":
            res = {"tools": tools}
        elif m == "tools/call":
            out = call(req["params"]["name"], req["params"].get("arguments", {}))
            res = {"content": [{"type": "text", "text": json.dumps(out)}], "isError": isinstance(out, dict) and "error" in out}
        else:
            if rid is None:
                continue                               # notifications get no response
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "method not found"}}) + "\n")
            sys.stdout.flush()
            continue
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": rid, "result": res}) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
