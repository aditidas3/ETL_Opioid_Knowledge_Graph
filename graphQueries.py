from neo4j import GraphDatabase
import json, time
from typing import Any, Dict, List, Union


def ensure_list(x: Union[None, Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


# ----------------- Upsert helpers ----------------- #

def upsert_case(tx, case_obj: Dict[str, Any]):
    case_id = case_obj.get("identifier")
    if not case_id:
        return

    tx.run(
        """
        MERGE (c:Case {identifier: $identifier})
        SET
          c.semantic_type = $semantic_type,
          c.legalStatus = $legalStatus,
          c.dateFiled = $dateFiled,
          c.confidentialityNotice = $confidentialityNotice,
          c.language = $language
        """,
        identifier=case_id,
        semantic_type=case_obj.get("semantic_type"),
        legalStatus=case_obj.get("legalStatus"),
        dateFiled=case_obj.get("dateFiled"),
        confidentialityNotice=case_obj.get("confidentialityNotice"),
        language=case_obj.get("language"),
    )

    # Case-level mentions
    for mention in case_obj.get("mentions") or []:
        if isinstance(mention, dict):
            upsert_case_mention(tx, case_id, mention)

    # hasPart emails
    for email_obj in ensure_list(case_obj.get("hasPart")):
        if isinstance(email_obj, dict):
            upsert_email_recursive(tx, case_id, email_obj, parent_email_id=None)


def upsert_case_mention(tx, case_id: str, mention: Dict[str, Any]):
    m_type = mention.get("@type")
    name = mention.get("name")
    if not name:
        return

    sem = mention.get("semantic_type")
    identifier = mention.get("identifier")

    if m_type == "gpe":
        label = "Place"
    else:
        label = "TopicEntity"

    # Node
    tx.run(
        f"""
        MERGE (m:{label} {{name: $name}})
        SET
          m.semantic_type = $semantic_type,
          m.identifier = $identifier
        """,
        name=name,
        semantic_type=sem,
        identifier=identifier,
    )

    # Relationship
    tx.run(
        f"""
        MATCH (c:Case {{identifier: $case_id}})
        MATCH (m:{label} {{name: $name}})
        MERGE (c)-[:CASE_MENTIONS]->(m)
        """,
        case_id=case_id,
        name=name,
    )


def upsert_person(tx, person: Dict[str, Any]) -> str:
    if not person:
        return None

    name = person.get("name") or "Unknown"
    email_addr = person.get("email")
    sem = person.get("semantic_type")
    key = email_addr or name

    tx.run(
        """
        MERGE (p:Person {key: $key})
        SET
          p.name = $name,
          p.email = $email,
          p.semantic_type = $semantic_type
        """,
        key=key,
        name=name,
        email=email_addr,
        semantic_type=sem,
    )

    aff = person.get("affiliation")
    if isinstance(aff, dict):
        upsert_org_for_person(tx, key, aff)

    return key


def upsert_org_for_person(tx, person_key: str, org: Dict[str, Any]):
    name = org.get("name")
    if not name:
        return

    role = org.get("role")
    sem = org.get("semantic_type")

    # Org node
    tx.run(
        """
        MERGE (o:Organization {name: $name})
        SET
          o.semantic_type = $semantic_type,
          o.role = $role
        """,
        name=name,
        semantic_type=sem,
        role=role,
    )

    # Person -> Org
    tx.run(
        """
        MATCH (p:Person {key: $person_key})
        MATCH (o:Organization {name: $name})
        MERGE (p)-[:AFFILIATED_WITH]->(o)
        """,
        person_key=person_key,
        name=name,
    )

    parent = org.get("parentOrganization")
    if isinstance(parent, dict) and parent.get("name"):
        pname = parent.get("name")
        psem = parent.get("semantic_type")
        prole = parent.get("role")

        # Parent org
        tx.run(
            """
            MERGE (po:Organization {name: $pname})
            SET
              po.semantic_type = $p_sem,
              po.role = $p_role
            """,
            pname=pname,
            p_sem=psem,
            p_role=prole,
        )

        # Org -> Parent
        tx.run(
            """
            MATCH (o:Organization {name: $name})
            MATCH (po:Organization {name: $pname})
            MERGE (o)-[:SUBSIDIARY_OF]->(po)
            """,
            name=name,
            pname=pname,
        )


def upsert_mention_for_email(tx, email_id: str, mention: Dict[str, Any]):
    m_type = mention.get("@type")
    name = mention.get("name")
    if not name:
        return

    sem = mention.get("semantic_type")
    identifier = mention.get("identifier")
    role = mention.get("role")

    if m_type == "gpe":
        label = "Place"
        rel_type = "EMAIL_MENTIONS_PLACE"
    else:
        label = "TopicEntity"
        rel_type = "EMAIL_MENTIONS_TOPIC"

    # Node
    tx.run(
        f"""
        MERGE (m:{label} {{name: $name}})
        SET
          m.semantic_type = $semantic_type,
          m.identifier = $identifier,
          m.role = $role
        """,
        name=name,
        semantic_type=sem,
        identifier=identifier,
        role=role,
    )

    # Relationship
    tx.run(
        f"""
        MATCH (e:Email {{identifier: $email_id}})
        MATCH (m:{label} {{name: $name}})
        MERGE (e)-[:{rel_type}]->(m)
        """,
        email_id=email_id,
        name=name,
    )


def upsert_attachment(tx, email_id: str, case_id: str, attachment: Dict[str, Any]):
    name = attachment.get("name")
    if not name:
        return

    sem = attachment.get("semantic_type")
    file_format = attachment.get("fileFormat")
    desc = attachment.get("description")

    # Document node
    tx.run(
        """
        MERGE (d:Document {name: $name})
        SET
          d.semantic_type = $semantic_type,
          d.fileFormat = $fileFormat,
          d.description = $description
        """,
        name=name,
        semantic_type=sem,
        fileFormat=file_format,
        description=desc,
    )

    # Email–Document
    tx.run(
        """
        MATCH (e:Email {identifier: $email_id})
        MATCH (d:Document {name: $name})
        MERGE (e)-[:HAS_ATTACHMENT]->(d)
        """,
        email_id=email_id,
        name=name,
    )

    # Case–Document
    if case_id:
        tx.run(
            """
            MATCH (c:Case {identifier: $case_id})
            MATCH (d:Document {name: $name})
            MERGE (c)-[:CASE_HAS_DOCUMENT]->(d)
            """,
            case_id=case_id,
            name=name,
        )


def upsert_email_recursive(tx, case_id: str, email_obj: Dict[str, Any], parent_email_id: str = None):
    if not email_obj:
        return

    email_id = email_obj.get("identifier")
    if not email_id:
        email_id = f"{email_obj.get('subject', 'Unknown')}|{email_obj.get('dateSent', '')}"

    # Email node
    tx.run(
        """
        MERGE (e:Email {identifier: $identifier})
        SET
          e.semantic_type = $semantic_type,
          e.subject = $subject,
          e.dateSent = $dateSent,
          e.importance = $importance,
          e.body = $body
        """,
        identifier=email_id,
        semantic_type=email_obj.get("semantic_type"),
        subject=email_obj.get("subject"),
        dateSent=email_obj.get("dateSent"),
        importance=email_obj.get("importance"),
        body=email_obj.get("body"),
    )

    # Case–Email
    if case_id:
        tx.run(
            """
            MATCH (c:Case {identifier: $case_id})
            MATCH (e:Email {identifier: $email_id})
            MERGE (c)-[:HAS_EMAIL]->(e)
            """,
            case_id=case_id,
            email_id=email_id,
        )

    # Parent email relationship (for forwarded messages)
    if parent_email_id:
        tx.run(
            """
            MATCH (parent:Email {identifier: $parent_id})
            MATCH (child:Email {identifier: $email_id})
            MERGE (parent)-[:FORWARDED_MESSAGE]->(child)
            """,
            parent_id=parent_email_id,
            email_id=email_id,
        )

    # Sender
    sender = email_obj.get("sender")
    if isinstance(sender, dict):
        sender_key = upsert_person(tx, sender)
        if sender_key:
            tx.run(
                """
                MATCH (e:Email {identifier: $email_id})
                MATCH (p:Person {key: $sender_key})
                MERGE (p)-[:SENT]->(e)
                """,
                email_id=email_id,
                sender_key=sender_key,
            )

    # Recipients
    for rcpt in email_obj.get("recipient") or []:
        if isinstance(rcpt, dict):
            rcpt_key = upsert_person(tx, rcpt)
            if rcpt_key:
                tx.run(
                    """
                    MATCH (e:Email {identifier: $email_id})
                    MATCH (p:Person {key: $rcpt_key})
                    MERGE (e)-[:SENT_TO]->(p)
                    """,
                    email_id=email_id,
                    rcpt_key=rcpt_key,
                )

    # Mentions
    for mention in email_obj.get("mentions") or []:
        if isinstance(mention, dict):
            upsert_mention_for_email(tx, email_id, mention)

    # Attachments
    for att in email_obj.get("attachments") or []:
        if isinstance(att, dict):
            upsert_attachment(tx, email_id, case_id, att)

    # Forwarded / nested
    fwd = email_obj.get("forwardedMessage")
    if isinstance(fwd, dict):
        upsert_email_recursive(tx, case_id, fwd, parent_email_id=email_id)

    # mentionsEmail
    for me in email_obj.get("mentionsEmail") or []:
        if isinstance(me, dict):
            ref_id = me.get("identifier")
            if ref_id:
                tx.run(
                    """
                    MERGE (ref:Email {identifier: $ref_id})
                    """,
                    ref_id=ref_id,
                )
                tx.run(
                    """
                    MATCH (e:Email {identifier: $email_id})
                    MATCH (ref:Email {identifier: $ref_id})
                    MERGE (e)-[:MENTIONS_EMAIL]->(ref)
                    """,
                    email_id=email_id,
                    ref_id=ref_id,
                )


# ----------------- Main import with logging & error handling ----------------- #

def import_jsonl_to_neo4j(
    jsonl_path: str,
    uri: str,
    user: str,
    password: str,
    log_every: int = 25,
):
    """
    Import JSONL case/email schemas into Neo4j with:
      - progress logging every `log_every` lines
      - per-line try/except so a bad record doesn't kill the whole run
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    total_lines = 0
    success_cases = 0
    skipped_lines = 0
    failed_cases = 0

    with driver.session() as session:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            start_time = time.time()
            for line_no, line in enumerate(f, start=1):
                total_lines += 1
                line = line.strip()
                if not line:
                    skipped_lines += 1
                    continue

                # Progress log
                if line_no % log_every == 0:
                    print(f"[INFO] Processing line {line_no}... (success={success_cases}, failed={failed_cases}, skipped={skipped_lines})")
                    print('\t took', time.time() - start_time, 'seconds')

                try:
                    wrapper = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[WARN] Skipping line {line_no}: invalid JSON wrapper ({e})")
                    skipped_lines += 1
                    continue

                output_raw = wrapper.get("output")
                if not output_raw:
                    print(f"[WARN] Skipping line {line_no}: no 'output' field")
                    skipped_lines += 1
                    continue

                try:
                    case_obj = json.loads(output_raw)
                except json.JSONDecodeError:
                    if isinstance(output_raw, dict):
                        case_obj = output_raw
                    else:
                        print(f"[WARN] Skipping line {line_no}: invalid 'output' JSON")
                        skipped_lines += 1
                        continue

                case_id = case_obj.get("identifier")

                # Wrap the write in try/except so a single bad case doesn't kill everything
                try:
                    def work(tx):
                        upsert_case(tx, case_obj)

                    session.execute_write(work)
                    success_cases += 1

                except Exception as e:
                    failed_cases += 1
                    print(f"[ERROR] Failed to import case on line {line_no} (case_id={case_id!r}): {type(e).__name__}: {e}")

    driver.close()

    print("\n=== Import summary ===")
    print(f"Total lines read:     {total_lines}")
    print(f"Successful cases:     {success_cases}")
    print(f"Failed cases:         {failed_cases}")
    print(f"Skipped lines:        {skipped_lines}")
    print('Runtime (s):          ', time.time() - start_time)



# ================== OVERRIDES / ADD-ONS FOR RXNORM, CROSSREF, ENRICHED CONTENT ==================

def upsert_cross_reference_email(tx, source_email_id: str, target_email_id: str, similarity_score: float = None):
    """Create or update a REFERS_TO_EMAIL relationship between two Email nodes.

    source_email_id and target_email_id correspond to Email.identifier values.
    For crossRefInfo entries that only have a CID, we still store them as Email nodes
    keyed by that CID so they can be used for graph analysis later.
    """
    if not source_email_id or not target_email_id:
        return

    tx.run(
        """
        MERGE (src:Email {identifier: $source_id})
        MERGE (tgt:Email {identifier: $target_id})
        MERGE (src)-[r:REFERS_TO_EMAIL]->(tgt)
        SET r.similarity_score = $similarity_score
        """.strip(),
        source_id=source_email_id,
        target_id=target_email_id,
        similarity_score=similarity_score,
    )


def upsert_rxnorm_drug_for_email(tx, email_id: str, drug):
    """Create/link RxNorm drug nodes for an email.

    The JSON for this project uses case/email-level keys like:
      "drugsRXnorm": ["Oxycontin", "MS Contin", ...]

    which are typically simple strings. We treat any string as the drug name.
    If a dict is provided, we look for name/rxcui/source fields.
    """
    if drug is None:
        return

    if isinstance(drug, str):
        name = drug.strip()
        rxcui = None
        source = None
    elif isinstance(drug, dict):
        name = (drug.get("name") or drug.get("drug_name") or drug.get("id") or "").strip()
        rxcui = drug.get("rxcui") or drug.get("rxnorm_id")
        source = drug.get("source") or drug.get("origin") or "RxNorm"
    else:
        return

    if not name:
        return

    # Node for the drug
    tx.run(
        """
        MERGE (d:RxNormDrug {name: $name})
        SET
          d.rxnorm_id = coalesce(d.rxnorm_id, $rxcui),
          d.source    = coalesce(d.source, $source)
        """.strip(),
        name=name,
        rxcui=rxcui,
        source=source,
    )

    # Relationship from Email -> drug (separate query to avoid SET-before-MATCH issues)
    tx.run(
        """
        MATCH (e:Email {identifier: $email_id})
        MATCH (d:RxNormDrug {name: $name})
        MERGE (e)-[:EMAIL_MENTIONS_DRUG]->(d)
        """.strip(),
        email_id=email_id,
        name=name,
    )


def upsert_enriched_content_for_email(tx, email_id: str, enriched):
    """Create enriched-content nodes for a given Email and link them.

    The qwen_output_v1.jsonl schema uses keys like:
      decisions_made      -> list of strings (Decision)
      concerns_raised     -> list of strings (Concern)
      events_mentioned    -> list of strings (Event)
      financial_mentions  -> list of strings (Financial)
      locations_mentioned -> list of strings or dicts with {name}
      people_mentioned    -> list of strings or dicts with {name, email}

    This helper maps them to:
      (Email)-[:HAS_DECISION]->(Decision {text})
      (Email)-[:HAS_CONCERN]->(Concern {text})
      (Email)-[:HAS_EVENT]->(Event {text})
      (Email)-[:HAS_FINANCIAL]->(Financial {text})
      (Email)-[:EMAIL_MENTIONS_LOCATION]->(Location {name})
      (Email)-[:MENTIONS_PERSON_ENRICHED]->(Person)
    """
    if not enriched:
        return

    def _create_text_nodes(label: str, rel_type: str, items):
        for item in items or []:
            if item is None:
                continue
            if isinstance(item, dict):
                text = (item.get("text") or item.get("value") or "").strip()
                source = item.get("source")
            else:
                text = str(item).strip()
                source = None
            if not text:
                continue
            tx.run(
                f"""
                MATCH (e:Email {{identifier: $email_id}})
                CREATE (n:{label} {{text: $text, source: $source}})
                MERGE (e)-[:{rel_type}]->(n)
                """.strip(),
                email_id=email_id,
                text=text,
                source=source,
            )

    # Decisions, concerns, events, financials
    _create_text_nodes("Decision",  "HAS_DECISION",   enriched.get("decisions_made"))
    _create_text_nodes("Concern",   "HAS_CONCERN",    enriched.get("concerns_raised"))
    _create_text_nodes("Event",     "HAS_EVENT",      enriched.get("events_mentioned"))
    _create_text_nodes("Financial", "HAS_FINANCIAL",  enriched.get("financial_mentions"))

    # Locations
    for loc in enriched.get("locations_mentioned") or []:
        if loc is None:
            continue
        if isinstance(loc, dict):
            name = (loc.get("name") or "").strip()
            source = loc.get("source")
        else:
            name = str(loc).strip()
            source = None
        if not name:
            continue
        # First ensure/update Location node
        tx.run(
            """
            MERGE (l:Location {name: $name})
            SET l.source = coalesce(l.source, $source)
            """.strip(),
            name=name,
            source=source,
        )
        # Then link Email -> Location in a separate query
        tx.run(
            """
            MATCH (e:Email {identifier: $email_id})
            MATCH (l:Location {name: $name})
            MERGE (e)-[:EMAIL_MENTIONS_LOCATION]->(l)
            """.strip(),
            email_id=email_id,
            name=name,
        )

    # People mentioned (optional enrichment on top of sender/recipient graph)
    for pm in enriched.get("people_mentioned") or []:
        if pm is None:
            continue
        if isinstance(pm, dict):
            name = (pm.get("name") or "").strip()
            email_addr = pm.get("email") or None
        else:
            name = str(pm).strip()
            email_addr = None
        if not name:
            continue

        person_dict = {"name": name, "email": email_addr, "semantic_type": "Person"}
        person_key = upsert_person(tx, person_dict)
        if person_key:
            tx.run(
                """
                MATCH (p:Person {key: $key})
                MATCH (e:Email {identifier: $email_id})
                MERGE (e)-[:MENTIONS_PERSON_ENRICHED]->(p)
                """.strip(),
                key=person_key,
                email_id=email_id,
            )


def upsert_email_recursive(tx, case_id: str, email_obj: Dict[str, Any], parent_email_id: str = None):
    """Override of the earlier upsert_email_recursive to add RxNorm, cross-ref, and enriched content.

    This function is designed against the qwen_output_v1.jsonl email schema, where each email object
    (under case_obj['hasPart'] or forwardedMessage) looks roughly like:

      {
        "@type": "email:EmailMessage",
        "semantic_type": "Email Communication",
        "identifier": "Email-1996-09-04-0909-RS",
        "subject": "...",
        "dateSent": "...",
        "importance": "High",
        "sender": {...},
        "recipient": [...],
        "body": "...",
        "mentions": [...],
        "attachments": [...],
        "forwardedMessage": {... or [..]},
        "mentionsEmail": [...],
        "crossRefInfo": {"crossRefEmails": [{"cid": "htcf0232", "score": 0.58}, ...]},
        "drugsRXnorm": ["Oxycontin", ...],
        "enriched_content": {...}
      }
    """
    if not email_obj:
        return

    # Robust email_id extraction
    email_id = email_obj.get("identifier") or email_obj.get("id")
    if not email_id:
        email_id = f"{email_obj.get('subject', 'Unknown')}|{email_obj.get('dateSent', '')}"

    # Core Email node
    tx.run(
        """
        MERGE (e:Email {identifier: $identifier})
        SET
          e.semantic_type = $semantic_type,
          e.subject       = $subject,
          e.dateSent      = $dateSent,
          e.importance    = $importance,
          e.body          = $body
        """.strip(),
        identifier=email_id,
        semantic_type=email_obj.get("semantic_type"),
        subject=email_obj.get("subject"),
        dateSent=email_obj.get("dateSent"),
        importance=email_obj.get("importance"),
        body=email_obj.get("body"),
    )

    # Link Email to its Case
    if case_id:
        tx.run(
            """
            MATCH (c:Case {identifier: $case_id})
            MATCH (e:Email {identifier: $email_id})
            MERGE (c)-[:HAS_EMAIL]->(e)
            """.strip(),
            case_id=case_id,
            email_id=email_id,
        )

    # Threading: forwarded / nested emails
    if parent_email_id:
        tx.run(
            """
            MATCH (parent:Email {identifier: $parent_id})
            MATCH (child:Email {identifier: $email_id})
            MERGE (parent)-[:FORWARDED_MESSAGE]->(child)
            """.strip(),
            parent_id=parent_email_id,
            email_id=email_id,
        )

    # Sender
    sender = email_obj.get("sender")
    if isinstance(sender, dict):
        sender_key = upsert_person(tx, sender)
        if sender_key:
            tx.run(
                """
                MATCH (e:Email {identifier: $email_id})
                MATCH (p:Person {key: $sender_key})
                MERGE (p)-[:SENT]->(e)
                """.strip(),
                email_id=email_id,
                sender_key=sender_key,
            )

    # Recipients
    for rcpt in email_obj.get("recipient") or []:
        if isinstance(rcpt, dict):
            rcpt_key = upsert_person(tx, rcpt)
            if rcpt_key:
                tx.run(
                    """
                    MATCH (e:Email {identifier: $email_id})
                    MATCH (p:Person {key: $rcpt_key})
                    MERGE (e)-[:SENT_TO]->(p)
                    """.strip(),
                    email_id=email_id,
                    rcpt_key=rcpt_key,
                )

    # Mentions (case/email-level entities like GPE, topicEntity)
    for mention in email_obj.get("mentions") or []:
        if isinstance(mention, dict):
            upsert_mention_for_email(tx, email_id, mention)

    # Attachments
    for att in email_obj.get("attachments") or []:
        if isinstance(att, dict):
            upsert_attachment(tx, email_id, case_id, att)

    # RxNorm drugs (email-level)
    for drug in email_obj.get("drugsRXnorm") or []:
        upsert_rxnorm_drug_for_email(tx, email_id, drug)

    # Enriched content
    enriched = email_obj.get("enriched_content") or {}
    if enriched:
        upsert_enriched_content_for_email(tx, email_id, enriched)

    # Forwarded / nested emails
    fwd = email_obj.get("forwardedMessage")
    if isinstance(fwd, dict):
        upsert_email_recursive(tx, case_id, fwd, parent_email_id=email_id)
    elif isinstance(fwd, list):
        for child in fwd:
            if isinstance(child, dict):
                upsert_email_recursive(tx, case_id, child, parent_email_id=email_id)

    # mentionsEmail -> cross-reference edges (identifier-based)
    for me in email_obj.get("mentionsEmail") or []:
        if isinstance(me, dict):
            ref_id = me.get("identifier")
            if ref_id:
                upsert_cross_reference_email(
                    tx,
                    source_email_id=email_id,
                    target_email_id=ref_id,
                    similarity_score=None,
                )

    # crossRefInfo.crossRefEmails -> cross-reference edges (CID-based with scores)
    cross = email_obj.get("crossRefInfo") or {}
    for cref in cross.get("crossRefEmails") or []:
        if isinstance(cref, dict):
            cid = cref.get("cid")  # CID string like "htcf0232"
            score = cref.get("score")
            if cid:
                upsert_cross_reference_email(
                    tx,
                    source_email_id=email_id,
                    target_email_id=cid,
                    similarity_score=score,
                )
