msg_json_schema_offer_response = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Consignment Offer Response Message",
    "type": "object",
    "required": ["offer_id", "client_id"],
    "properties": {
        "offer_id": {
            "type": "string",
            "description": "The id of the offer"
        },
        "client_id": {
            "type": "string",
            "description": "Client that want the contract for the offer"
        }
    }

}

msg_json_schema_contract = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Consignment Contract Message",
    "type": "object",
    "required": ["consignment_id", "task_name", "offer_id", "contract_id"],
    "properties": {
        "consignment_id": {
            "type": "string",
            "description": "The id of the consignment"
        },
        "task_name": {
            "type": "string",
            "description": "The task the consignment needs done"
        },
        "offer_id": {
            "type": "string",
            "description": "The id of the particular offer associated with consignment this contract is for"
        },
        "contract_id": {
            "type": "string",
            "description": "The id of the contract which is unique to the client"
        },
        "task_parameters": {
            "type": "object",
            "description": "The inputs for the task"
        },

    }
}

msg_json_schema_offer = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Consignment Offer Message",
    "type": "object",
    "required": ["offer_id", "task_name", "worker_parameters"],
    "properties": {
        "offer_id": {
            "type": "string",
            "description": "The id of the offer"
        },
        "task_name": {
            "type": "string",
            "description": "The task the offer need completed"
        },
        "worker_parameters": {
            "type": "object",
            "description": "The qualities a worker needs to complete the task"
        }
    }
}

msg_json_schema_heartbeat = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "HeartBeat Message",
    "type": "object",
    "required": ["client_id", "description"],
    "properties": {
        "client_id": {
            "type": "string",
            "description": "The id of the client"
        },
        "description": {
            "type": "object",
            "description": "Dictionary of descriptors keyed off of descriptor name"
        }
    }
}

msg_json_schema_task_results = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Results Message",
    "type": "object",
    "required": ["client_id", "offer_id", "results", "results_encoding", "work_sequence", "finished", "sent_timestamp_utc"],
    "properties": {
        "client_id": {
            "type": "string",
            "description": "The id of the client"
        },
        "offer_id": {
            "type": "string",
            "description": "The id of the consignment offer the client is working off from"
        },
        "results": {
            "type": ["string", "null"],
            "description": "The results of the task"
        },
        "results_encoding": {
            "type": ["string", "null"],
            "description": "The encoding of the result value"
        },
        "work_sequence": {
            "type": "integer",
            "description": "The place in the order of all results this message represents"
        },
        "finished": {
            "type": "boolean",
            "description": "The worker is done."
        },
        "sent_timestamp_utc": {
            "type": "string",
            "description": "The time the result was sent from the worker"
        }
    }
}
