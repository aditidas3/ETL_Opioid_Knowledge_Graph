######  this is a utility file to process emails ######

# download libraries
import ast, re, json, time, os, requests, datetime, time, spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from typing import Dict, Any, List
from pathlib import Path
from collections import Counter
from google.colab import drive

# function to add cross reference email Ids
def add_cross_references_emailIds(input_file: str,output_file: str,similarity_threshold: float):
    """
    Add cross-references email ids using email bodies
    """   
    # Load data
    if input_file.endswith('.jsonl'):
        with open(input_file, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
    else:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

    # Helper function to extract ALL bodies recursively
    def extract_all_bodies(email_obj):
        """Extract all email bodies including forwarded messages"""
        all_bodies = []
        
        if isinstance(email_obj, list):
            for email in email_obj:
                all_bodies.extend(extract_all_bodies(email))
        
        elif isinstance(email_obj, dict):
            # Get this email's body
            body = email_obj.get('body', '')
            if body and len(body.strip()) > 0:
                all_bodies.append(body)
            
            # Recursively get forwarded message bodies
            if 'forwardedMessage' in email_obj:
                all_bodies.extend(extract_all_bodies(email_obj['forwardedMessage']))
        
        return all_bodies
    
    # Extract texts and IDs    
    texts = []
    ids = []
    id_to_item_map = {}
    items_with_no_bodies = []
    
    for item in data:
        if 'output' in item and isinstance(item['output'], str):
            try:
                output_obj = json.loads(item['output'])
            except:
                output_obj = item
        else:
            output_obj = item.get('output', item)
        
        # Get hasPart
        has_part = output_obj.get('hasPart')
        
        if has_part:
            all_bodies = extract_all_bodies(has_part)
            if all_bodies:
                combined_body = ' '.join(all_bodies)
                
                item_id = item.get('email_id', len(texts))
                texts.append(combined_body)
                ids.append(item_id)
                id_to_item_map[item_id] = item
            else:
                items_with_no_bodies.append(item.get('email_id'))
        else:
            items_with_no_bodies.append(item.get('email_id'))
    
    print(f"Extracted {len(texts)} items with email bodies")
    if items_with_no_bodies:
      print(f"Skipped {len(items_with_no_bodies)} items without bodies: {items_with_no_bodies}\n")

    # Calculate similarity        
    vectorizer = TfidfVectorizer(stop_words='english', lowercase=True)
    tfidf = vectorizer.fit_transform(texts)
    sim = cosine_similarity(tfidf)    
    crossRefIds = {}
    total_refs = 0    
    for i in range(len(ids)):
        cross_refs = []
        for j in range(len(ids)):
            if i != j and sim[i, j] > similarity_threshold:
                cross_refs.append({
                    "cid": ids[j],
                    "score": round(float(sim[i, j]), 4)
                })
                total_refs += 1
        
        cross_refs.sort(key=lambda x: x['score'], reverse=True)
        crossRefIds[ids[i]] = cross_refs 
 
    for item in data:
        item_id = item.get('email_id')

        if item_id in crossRefIds:
            # Parse output
            if 'output' in item and isinstance(item['output'], str):
                try:
                    output_obj = json.loads(item['output'])
                except:
                    continue
            else:
                output_obj = item.get('output', item)
            
            # Add crossRefInfo
            has_part = output_obj.get('hasPart')
            
            if has_part:
                cross_ref_section = {
                    "crossRefEmails": crossRefIds[item_id],
                    "totalCrossRefs": len(crossRefIds[item_id])
                }
                
                if isinstance(has_part, dict):
                    has_part['crossRefInfo'] = cross_ref_section
                elif isinstance(has_part, list):
                    output_obj['crossRefInfo'] = cross_ref_section
                
                # Update output
                if 'output' in item and isinstance(item['output'], str):
                    item['output'] = json.dumps(output_obj, ensure_ascii=False, indent=2)
    
    # Save
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    if output_file.endswith('.jsonl'):
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
    else:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Cross-references added to {len(ids)} items!")
    
    return data, crossRefIds

# class to add rxnorm drugs list
class extractRXnormDrugs:
  def __init__(self,input_file:str,output_file:str):
    self.input_file = input_file
    self.output_file = output_file

  def is_valid_drug_term(self,term):
      """Filter out invalid drug terms"""
      # Minimum length
      if len(term) < 3:
          return False
      # Must contain at least one letter
      if not re.search(r'[a-zA-Z]', term):
          return False
      # Reject if it's mostly special characters
      special_chars = sum(1 for c in term if not c.isalnum() and c != ' ' and c != '-')
      if special_chars > len(term) * 0.3:  # More than 30% special chars
          return False
      # Reject emails
      if '@' in term or '.com' in term or '.org' in term:
          return False
      # Reject common titles
      titles = ['Rep.', 'Dr.', 'Mr.', 'Mrs.', 'Ms.', 'Prof.']
      if any(title in term for title in titles):
          return False
      # Reject numbers-only or mostly numbers
      if term.replace('.', '').replace(',', '').isdigit():
          return False
      return True

  # Load Spacy model with entity recognition
  def extract_chemicals_with_spacy(self,text):
    doc = nlp(text)
    chemicals = []
    for ent in doc.ents:
      if ent.label_ in ["CHEMICAL", "DRUG"]:
        term = ent.text.strip()
        # Filter out noise
        if self.is_valid_drug_term(term):
          chemicals.append(term)
    return chemicals

  def get_drug_name_from_rxcui(self,rxcui):
      """Get the drug name directly from RXCUI"""
      url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json"
      try:
          r = requests.get(url)
          result = r.json()
          properties = result.get("properties", {})
          name = properties.get("name")
          return name
      except Exception as e:
          return None

  def rxnorm_match(self,term):
      """Get RXCUI for a chemical/drug term"""
      url = "https://rxnav.nlm.nih.gov/REST/approximateTerm.json"
      try:
          r = requests.get(url, params={"term": term, "maxEntries": 1})
          result = r.json()
          candidates = result.get("approximateGroup", {}).get("candidate", [])
          return candidates[0].get("rxcui")
      except:
          return None

  def extract_unique_chemical_terms(self):
    all_terms = set()
    text_to_candidates = {}
    # Load data
    if self.input_file.endswith('.jsonl'):
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
    else:
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    
    # Helper function to extract ALL bodies recursively
    def extract_all_bodies(email_obj):
        """Extract all email bodies including forwarded messages"""
        all_bodies = []
        
        if isinstance(email_obj, list):
            for email in email_obj:
                all_bodies.extend(extract_all_bodies(email))
        
        elif isinstance(email_obj, dict):
            # Get this email's body
            body = email_obj.get('body', '')
            if body and len(body.strip()) > 0:
                all_bodies.append(body)
            
            # Recursively get forwarded message bodies
            if 'forwardedMessage' in email_obj:
                all_bodies.extend(extract_all_bodies(email_obj['forwardedMessage']))
        
        return all_bodies
    
    for item in data:
      if 'output' in item and isinstance(item['output'], str):
          try:
              output_obj = json.loads(item['output'])
          except:
              output_obj = item
      else:
          output_obj = item.get('output', item)
      
      # Get hasPart
      has_part = output_obj.get('hasPart')
      
      if has_part:
        all_bodies = extract_all_bodies(has_part)
        if all_bodies:
          combined_body = ' '.join(all_bodies)
          candidates = self.extract_chemicals_with_spacy(combined_body)
          identifier = item.get('email_id')
          if identifier not in text_to_candidates:
            text_to_candidates[identifier] = []
          text_to_candidates[identifier].extend(candidates)
          all_terms.update(candidates)
    return all_terms,text_to_candidates,data
        
  def parse_rxnorm(self,all_terms):
    term_to_drugs = {}
    for i,term in enumerate(all_terms):
      rxcui = self.rxnorm_match(term)
      if rxcui:
        drug_names = self.get_drug_name_from_rxcui(rxcui)
        if drug_names:
          term_to_drugs[term] = drug_names
    return term_to_drugs

  def add_rxnorm_drugs_name(self):
      all_terms,text_to_candidates,data = self.extract_unique_chemical_terms()
      term_to_drugs = self.parse_rxnorm(all_terms)
      
      for item in data:
          if 'output' in item and isinstance(item['output'], str):
              try:
                  output_obj = json.loads(item['output'])
              except:
                  output_obj = item
          else:
              output_obj = item.get('output', item)
          
          # Get hasPart
          has_part = output_obj.get('hasPart')
          identifier = item.get('email_id')
          candidates = text_to_candidates.get(identifier,[])
          all_drug_name = [] #collect all drug name
          for term in candidates:
            drug_name = term_to_drugs.get(term)
            if drug_name:
              all_drug_name.append(drug_name)

          if all_drug_name:
            if has_part:
              unique_drugs = sorted(list(set(all_drug_name)))
              if isinstance(has_part, dict):
                has_part['drugsRXnorm'] = unique_drugs
              elif isinstance(has_part, list):
                output_obj['drugsRXnorm'] = unique_drugs
              
              if 'output' in item and isinstance(item['output'], str):
                item['output'] = json.dumps(output_obj, ensure_ascii=False, indent=2)
    
      # Save
      Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)
      
      if self.output_file.endswith('.jsonl'):
          with open(output_file, 'w', encoding='utf-8') as f:
              for item in data:
                  f.write(json.dumps(item, ensure_ascii=False) + '\n')
      else:
          with open(self.output_file, 'w', encoding='utf-8') as f:
              json.dump(data, f, ensure_ascii=False, indent=2)
      print('File saved successfully')
      return data

# class to extract semantic entity using qwen api
class QwenEntityExtractor:

  def __init__(self, api_key: str, model:str):  
    self.api_key = api_key
    self.base_url = "https://openrouter.ai/api/v1/chat/completions"
    self.model = model
    self.rate_limit_delay = 1 # 1 second delay between each api requests

  def extract_body_info(self, body_text: str, context: Dict = None) -> Dict[str, Any]:
    context_str = ""
    if context:
      context_str = f"\nContext: {json.dumps(context, indent=2)}"

    prompt = f"""Analyze the following email body text and extract structured information.{context_str}

    Email Body:
    {body_text}

    Extract and return a JSON object with the following fields:
    1. "decisions_made": Array of decisions or conclusions
    2. "concerns_raised": Array of concerns, risks, or issues mentioned
    3. "people_mentioned": Array of people mentioned (beyond sender/recipient)
    4. "locations_mentioned": Array of geographic locations mentioned
    5. "events_mentioned": Array of events mentioned
    6. "financial_mentions": Any financial figures, costs, or budget items mentioned

    Return ONLY the JSON object, no additional text or markdown formatting."""

    headers = {
      "Authorization": f"Bearer {self.api_key}",
      "Content-Type": "application/json"
    }

    payload = {
      "model": self.model,
      "messages": [
        {
          "role": "system",
          "content": "You are an expert at analyzing email content and extracting structured information. Always return valid JSON only."
        },
        {
          "role": "user",
          "content": prompt
        }
      ],
      "temperature": 0.3,
      "max_tokens": 1000
    }

    try:
      response = requests.post(self.base_url, headers=headers, json=payload, timeout=30)
      response.raise_for_status()

      result = response.json()
      content = result['choices'][0]['message']['content']

      # Clean up the response
      content = content.strip()
      if content.startswith('```json'):
          content = content[7:]
      if content.startswith('```'):
          content = content[3:]
      if content.endswith('```'):
          content = content[:-3]

      extracted_info = json.loads(content.strip())
      return extracted_info

    except Exception as e:
      print(f"Error extracting information: {e}")
      return {
        "decisions_made": [],
        "concerns_raised": [],
        "people_mentioned": [],
        "locations_mentioned": [],
        "events_mentioned": [],
        "financial_mentions": [],
        "error": str(e)
      }

  def process_email_object(self, email_obj: Dict) -> tuple:
    api_calls = 0
    if not email_obj or '@type' not in email_obj:
      return email_obj, api_calls

    # Process if it's an email message
    if 'EmailMessage' in email_obj.get('@type', ''):
      body = email_obj.get('body', '')

      if body and len(body.strip()) > 0:
        context = {
            "sender": email_obj.get('sender', {}).get('name', 'Unknown'),
            "date_sent": email_obj.get('dateSent', ''),
            "subject": email_obj.get('subject', '')
        }
        extracted = self.extract_body_info(body, context)
        email_obj['enriched_content'] = extracted
        api_calls += 1

        # Rate limiting
        time.sleep(self.rate_limit_delay)

    # Recursively process forwarded messages
    if 'forwardedMessage' in email_obj:
      email_obj['forwardedMessage'], forwarded_calls = self.process_email_object(
        email_obj['forwardedMessage']
      )
      api_calls += forwarded_calls

    return email_obj, api_calls

  def split_into_batches(self, input_file: str, output_dir: str):
    if input_file.endswith('.jsonl'):
      with open(input_file, 'r', encoding='utf-8') as f:
        data = [json.loads(line) for line in f]
    else:
      with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
      data = [data]

    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)
    items_per_batch = 10 

    print(f"\nBatch Planning:")
    print(f"   Total items: {len(data)}")
    print(f"   Items per batch: {items_per_batch}")
    print(f"   Total batches needed: {(len(data) + items_per_batch - 1) // items_per_batch}")

    batch_files = []
    for i in range(0, len(data), items_per_batch):
      batch_num = (i // items_per_batch) + 1
      batch = data[i:i+items_per_batch]
      batch_filename = f"{output_dir}/batch_{batch_num:03d}.json"
      with open(batch_filename, 'w', encoding='utf-8') as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)

      batch_files.append(batch_filename)
      
    print(f"\nCreated {len(batch_files)} batch files in '{output_dir}/' directory\n")
    return batch_files

  def process_batch(self, batch_file: str, output_file: str):
        """Process a single batch file"""

        start_time = datetime.datetime.now()

        with open(batch_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        enriched_data = []
        total_api_calls = 0
        total_items = len(data)
        
        for idx, item in enumerate(data, 1):
            print(f"Processing item {idx}/{total_items} (Id: {item.get('email_id', 'N/A')})...")

            # Parse the output field if it's a string
            if 'output' in item and isinstance(item['output'], str):
                try:
                    output_obj = json.loads(item['output'])
                except json.JSONDecodeError as e:
                    print(f"Error parsing output JSON: {e}")
                    enriched_data.append(item)
                    continue
            else:
                output_obj = item

            # Process hasPart
            has_part = output_obj.get('hasPart')
            item_api_calls = 0

            if has_part:
                if isinstance(has_part, list):
                    # Process array of emails
                    for email_idx, email in enumerate(has_part):
                        print(f"Processing email {email_idx + 1}/{len(has_part)}...")
                        has_part[email_idx], calls = self.process_email_object(email)
                        item_api_calls += calls
                elif isinstance(has_part, dict):
                    # Process single email
                    output_obj['hasPart'], item_api_calls = self.process_email_object(has_part)

            # Reconstruct the item
            if 'output' in item and isinstance(item['output'], str):
                item['output'] = json.dumps(output_obj, ensure_ascii=False, indent=2)

            enriched_data.append(item)
            total_api_calls += item_api_calls

        # Save enriched data
        print(f"\nSaving enriched data to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_data, f, ensure_ascii=False, indent=2)

        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\nBATCH COMPLETE!")
        print(f"   Time taken: {duration/3600:.2f} hours ({duration/60:.1f} minutes)")

        return total_api_calls

# class to re-process failed batches
class reprocessFailedBatch:
  def __init__(self,api_key):
    self.api_key2 = api_key2
  
  # find out the failed batch
  def find_error_inBatches(self,enriched_folder: str):
    errors_files = []   
    enriched_path = Path(enriched_folder)    
    batch_files = sorted(f for f in enriched_path.glob("enriched_batch_*.json") if not str(f).endswith("_failed.json"))    
    print(f"Scanning {len(batch_files)} enriched batch files for errors...\n")    
    for batch_file in batch_files:
      batch_has_error = False 
      with open(batch_file, 'r', encoding='utf-8') as f:
        data = json.load(f)        
      for item in data:
        if 'output' in item and isinstance(item['output'], str):
            try:
                output_obj = json.loads(item['output'])
            except:
                output_obj = item
        else:
            output_obj = item.get('output', item)
        
        # Check hasPart for errors
        has_part = output_obj.get('hasPart')                    
        if has_part:
          if isinstance(has_part, list):
            for sub_item in has_part:
              enriched = sub_item.get('enriched_content', {})
              if isinstance(enriched, dict) and 'error' in enriched and enriched['error']:
                  batch_has_error = True
                  break  # Break inner loop
          elif isinstance(has_part, dict):
            enriched = has_part.get('enriched_content', {})
            if isinstance(enriched, dict) and 'error' in enriched and enriched['error']:
                batch_has_error = True
        if batch_has_error:
          break  # no need to check further, this batch has at least one failed index
      if batch_has_error:
        errors_files.append(batch_file.name)
        print(f"{batch_file.name} - Has errors")
    return errors_files  

  def reprocess_failed_batches(self,batch_dir:str,enriched_dir:str):
    errors = self.find_error_inBatches(f"{enriched_dir}")
    if not errors:
      print("No errors found to preprocess!")
      return None

    reprocessor = QwenEntityExtractor(api_key=api_key2)
    for failed_filename in errors:
      failed_file_path = f"{enriched_dir}/{failed_filename}"
      batch_num = failed_filename.split("_")[-1].split(".")[0] #extracting batch number
      print(f"\nRe-processing batch {batch_num}")
      # Renaming old failed enriched file
      failed_backup = failed_file_path.replace(".json", "_failed.json")
      os.rename(failed_file_path, failed_backup)
      # Getting original batch file for reprocessing
      input_batch = f"{batch_dir}/batch_{batch_num}.json"
      # Output new enriched file
      output_batch = f"{enriched_dir}/enriched_batch_{batch_num}.json"
      # Re-run the extractor
      calls = reprocessor.process_batch(
        batch_file=input_batch,
        output_file=output_batch
      )
      print(f"Completed reprocessing batch {batch_num}")

# function to merge batch class into single jsonl file
def merge_batches_to_jsonl(enriched_folder: str, output_file: str): 
    enriched_path = Path(enriched_folder)
    batch_files = sorted(f for f in enriched_path.glob("enriched_batch_*.json") if not str(f).endswith("_failed.json"))
    
    print(f"Found {len(batch_files)} batch files to merge\n")
    all_items = []    
    for batch_file in batch_files:        
        with open(batch_file, 'r', encoding='utf-8') as f:
            batch_data = json.load(f)        
        all_items.extend(batch_data)
      
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in all_items:
            json_line = json.dumps(item, ensure_ascii=False)
            f.write(json_line + '\n')    
    return all_items














