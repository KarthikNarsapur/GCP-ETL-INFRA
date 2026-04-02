"""
Batch Prompt Builders

Helpers for modifying system prompts to remove tool references for batch inference.
"""
import re


def build_batch_system_prompt(base_prompt: str, step_type: str) -> str:
    """
    Remove tool references and add batch-specific instructions.
    
    Args:
        base_prompt: Original system prompt with tool references
        step_type: extraction, data_rules, match_rules, ping
    
    Returns:
        Modified prompt without tool references, with batch-specific instructions
    """
    prompt = base_prompt
    
    if step_type == "extraction":
        # Remove calculator tool section
        prompt = re.sub(
            r'<tools_available>.*?</tools_available>',
            '',
            prompt,
            flags=re.DOTALL
        )
        
        # Remove calculator tool mentions
        prompt = re.sub(
            r'- calculator:.*?\n',
            '',
            prompt,
            flags=re.MULTILINE
        )
        
        # Remove tool efficiency rules
        prompt = re.sub(
            r'⚠️ TOOL EFFICIENCY RULES.*?PREFERRED:.*?\n',
            '',
            prompt,
            flags=re.DOTALL
        )
        
        # Add batch-specific instruction
        batch_instruction = """
⚠️ BATCH MODE INSTRUCTIONS:
- Perform all calculations mentally. Trust document values.
- If totals don't match, trust the document values and note discrepancies in extraction_notes.
- Do NOT attempt to use any tools - they are not available in batch mode.
"""
        # Insert after <task> or at the beginning if no <task>
        if "<task>" in prompt:
            prompt = prompt.replace("</task>", f"</task>{batch_instruction}")
        else:
            prompt = batch_instruction + "\n" + prompt
    
    elif step_type in ["data_rules", "match_rules"]:
        # Remove calculator tool section
        prompt = re.sub(
            r'## AVAILABLE TOOL:.*?calculator:.*?\n',
            '',
            prompt,
            flags=re.DOTALL
        )
        
        # Remove calculator usage instructions
        prompt = re.sub(
            r'3\. \*\*ONE CALCULATOR CALL\*\*.*?← This wastes.*?\n',
            '',
            prompt,
            flags=re.DOTALL
        )
        
        # Add batch-specific instruction
        batch_instruction = """
⚠️ BATCH MODE INSTRUCTIONS:
- Use the pre-computed expected values provided in the context.
- Compare document values against expected values.
- If difference exceeds tolerance, mark rule as failed.
- Perform calculations mentally using the provided values.
- Do NOT attempt to use any tools - they are not available in batch mode.
"""
        # Insert after validation instructions
        if "## OUTPUT:" in prompt:
            prompt = prompt.replace("## OUTPUT:", batch_instruction + "\n## OUTPUT:")
        else:
            prompt = prompt + "\n" + batch_instruction
        
        # Add final reinforcement for output format at the end
        output_reinforcement = """

═══════════════════════════════════════════════════════════════════════════════
⚠️ FINAL REMINDER - OUTPUT FORMAT IS CRITICAL
═══════════════════════════════════════════════════════════════════════════════
Your response MUST be a valid JSON object with EXACTLY this format:

{"results": [{"rule_id": "...", "rule_name": "...", "breach_level": "...", "passed": true/false, "user_output": "..."}]}

- The root key MUST be "results" (lowercase, plural)
- Do NOT use alternative keys like "validation_results", "rule_validations", "detailed_results"
- Do NOT include markdown, headers, or explanatory text
- Return ONLY the JSON object
═══════════════════════════════════════════════════════════════════════════════
"""
        prompt = prompt + output_reinforcement
    
    elif step_type == "ping":
        # Ping agent doesn't use tools, so no changes needed
        pass
    
    return prompt
