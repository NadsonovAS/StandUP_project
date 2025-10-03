import json
import subprocess
from typing import Any, Callable, Dict, Sequence

from config import get_settings
from utils import try_except_with_log

SUMMARY_PROMPT_TEMPLATE = """
### Role and Goal
**You:** An AI expert-analyst specializing in structuring conversational content, such as stand-up comedy.
**Your task:** Analyze the provided text and generate a single, valid JSON object that segments the text into main thematic blocks in accordance with all the rules listed below. Your output must contain **only** the JSON object and nothing else. Do not add any introductory phrases, explanations, or apologies, such as 'Here is the JSON you requested'.

### Critical Rules and Instructions
1. **Thematic Segmentation:**
- Identify large, self-contained thematic blocks in chronological order. A theme is a main subject of discussion that lasts for several minutes.
- **Do not split themes:** The same topic (for example, 'Stories About My Cat') should be represented as a single theme, not several small ones.
- **Do not merge themes:** Unrelated topics (for example, politics and then dating) should be separated into different themes.
    
2. **Content Requirements:**
- **id**: Use the **integer key** from the transcript (for example, "0", "1", "2", …) from which a new theme begins. The ids must be strictly in ascending order.
- **theme**: Theme titles must be in English, brief (2-5 words), and descriptive (for example, "Awkward First Dates").
- **summary**:
  - For all standard themes, write one detailed paragraph in English, with a length of **at least 50 words**. The summary should neutrally convey the main plot, key jokes, and the performer's point of view.
  - **CRITICAL EXCEPTION - Advertising:** This rule has the highest priority. If you find an advertising integration, the theme **must** be called "Advertising". The summary for such a theme must have a special format: it **MUST** begin with Company: <COMPANY NAME>, followed by a single sentence describing the advertisement (maximum 25 words).

### Output Format Specification
- The output must be a **single JSON object**.
- The root key must be "chapters", containing an array of objects.
- Each object in the array must contain three keys: id (integer), theme (string), and summary (string).

### Text for Analysis:

"""


CLASSIFIER_PROMPT_TEMPLATE = """
**Task:** Your task is to analyze the provided summaries and generate a single, valid JSON object that classifies each summary according to the given categories. The JSON output should be the only thing you generate.

**Role:** You are a strict and precise classification system for stand-up comedy content.

**Input:**
1.  **Categories List:** A JSON object containing the official  main_category  and  subcategory  list. You MUST use these categories exactly as provided.
2.  **Summaries Data:** A JSON object containing  id ,  theme , and  summary  for different segments of a stand-up show.

**Objective:** Classify each summary into exactly one  main_category  and one  subcategory  from the **Categories List**.

**Instructions & Rules:**

1.  **Strict Classification:**
*   For each summary from the **Summaries Data**, select exactly **one**  main_category  and, within it, exactly **one**  subcategory  from the provided **Categories List**.
*   If multiple categories seem applicable, choose the one that represents the most prominent and central topic of the summary.
*   **Do not** invent new categories or subcategories. The spelling and casing must match the **Categories List** perfectly.

2.  **Output Format:**
*   The output must be a single, valid JSON object.
*   The JSON object must have a single root key:  "classifications" .
*   The value of  "classifications"  must be an array of objects.
*   Each object in the array corresponds to a summary from the input and must contain:
    *    "id" : The original integer ID of the summary.
    *    "main_category" : The selected main category.
    *    "subcategory" : The selected subcategory.
    *    "reason" : A brief (1-2 sentences) explanation in English for your classification choice.

**Categories List:**
[
  {
    "main_category": "Advertising",
    "subcategories": [
      "Upcoming shows & live events",
      "Streaming platforms & online services",
      "Marketplaces & e-commerce",
      "Tech products & gadgets",
      "Food & beverages",
      "Travel & lifestyle services",
      "Finance & banking apps",
      "Health & fitness products",
      "Mobile operators & internet providers",
      "Education & online courses"
    ]
  },
  {
    "main_category": "Politics & Society",
    "subcategories": [
      "Political satire",
      "Social commentary & inequality",
      "Human rights & activism",
      "Immigration & migration",
      "Law, crime & justice",
      "Censorship & freedom of speech"
    ]
  },
  {
    "main_category": "Economy, Work & Money",
    "subcategories": [
      "Workplace humor & office culture",
      "Money & personal finance",
      "Career struggles & unemployment",
      "Business & entrepreneurship"
    ]
  },
  {
    "main_category": "Health, Body & Mind",
    "subcategories": [
      "Physical health & fitness",
      "Mental health & therapy",
      "Addictions & substance use",
      "Aging & body image"
    ]
  },
  {
    "main_category": "Relationships & Social Life",
    "subcategories": [
      "Dating & romance",
      "Marriage & family life",
      "Friendship & social circles",
      "Sex & intimacy",
      "Parenting"
    ]
  },
  {
    "main_category": "Science, Technology & Digital Life",
    "subcategories": [
      "Tech & gadgets",
      "Internet culture, memes & influencers",
      "Artificial intelligence & future tech",
      "Science news & discoveries"
    ]
  },
  {
    "main_category": "Culture, Arts & Media",
    "subcategories": [
      "Movies, TV & streaming",
      "Music & live performance",
      "Literature & art references",
      "Celebrities & fame",
      "Pop culture trends"
    ]
  },
  {
    "main_category": "Environment & Planet",
    "subcategories": [
      "Climate change & sustainability jokes",
      "Animals & pets",
      "Urban vs rural life",
      "Natural disasters & weather humor"
    ]
  },
  {
    "main_category": "History, Identity & Heritage",
    "subcategories": [
      "Historical events satire",
      "Cultural traditions & heritage",
      "Generational differences & nostalgia",
      "National stereotypes"
    ]
  },
  {
    "main_category": "Dark, Edgy & Absurd Humor",
    "subcategories": [
      "Morbid comedy & death jokes",
      "Offensive or taboo topics",
      "Self-deprecating humor",
      "Surreal or absurd comedy"
    ]
  }
]

**Summaries Data to Classify:**

"""


def default_run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def clean_json_output(llm_output: str) -> str:
    if "```json" in llm_output:
        return llm_output.split("```json", 1)[1].split("```", 1)[0].strip()
    if "```" in llm_output:
        return llm_output.split("```", 1)[1].split("```", 1)[0].strip()
    return llm_output.strip()


class GeminiClient:
    """Lightweight wrapper around the Gemini CLI for easier testing."""

    def __init__(
        self,
        *,
        run_command: Callable[
            [Sequence[str]], subprocess.CompletedProcess[str]
        ] = default_run_command,
        model: str | None = None,
        max_attempts: int = 2,
        command_builder: Callable[[str, str], Sequence[str]] | None = None,
    ) -> None:
        settings = get_settings()
        self._run_command = run_command
        self._model = model or settings.GEMINI_MODEL
        self._max_attempts = max_attempts
        self._command_builder = command_builder or self._default_command_builder

    def _default_command_builder(self, prompt: str, model: str) -> Sequence[str]:
        return ["gemini", "-p", prompt, "-m", model]

    @try_except_with_log()
    def request(self, prompt: str) -> Dict[str, Any] | None:
        current_prompt = prompt
        for _ in range(self._max_attempts):
            result = self._run_command(
                self._command_builder(current_prompt, self._model)
            )
            if result.returncode != 0:
                if "fetch failed" in result.stderr:
                    # This is a transient network error, try again
                    continue
                raise RuntimeError(f"Gemini CLI failed: {result.stderr}")

            llm_output = clean_json_output(result.stdout)
            try:
                return json.loads(llm_output)
            except json.JSONDecodeError as exc:
                current_prompt = (
                    prompt
                    + "Your previous response had a JSON formatting error: "
                    + f"{exc}.\n Here is the invalid response you provided:\n\n {llm_output} "
                    + "\n\n Please correct the JSON and provide the full, valid JSON object."
                )
        return None


def build_summary_prompt(transcribe_json: Dict[str, Dict[str, Any]]) -> str:
    remove_keys = {"start", "end"}
    filtered = {
        key: {k: v for k, v in value.items() if k not in remove_keys}
        for key, value in transcribe_json.items()
    }
    return SUMMARY_PROMPT_TEMPLATE + str(filtered)


def build_classifier_prompt(llm_chapter_json: Dict[str, Any]) -> str:
    sanitized = {
        "chapters": [
            {k: v for k, v in chapter.items() if k != "end_id"}
            for chapter in llm_chapter_json.get("chapters", [])
        ]
    }
    return CLASSIFIER_PROMPT_TEMPLATE + str(sanitized)


@try_except_with_log("Sending Gemini request for topic extraction")
def llm_summary(
    transcribe_json: Dict[str, Dict[str, Any]],
    *,
    client: GeminiClient | None = None,
) -> Dict[str, Any] | None:
    active_client = client or GeminiClient()
    response = active_client.request(build_summary_prompt(transcribe_json))
    if response is None:
        return None

    end_id = max(map(int, transcribe_json.keys())) if transcribe_json else 0
    for chapter in reversed(response.get("chapters", [])):
        chapter["end_id"] = end_id
        end_id = chapter["id"] - 1
    return response


@try_except_with_log("Sending Gemini request for topic classification")
def llm_classifier(
    llm_chapter_json: Dict[str, Any],
    *,
    client: GeminiClient | None = None,
) -> Dict[str, Any] | None:
    active_client = client or GeminiClient()
    response = active_client.request(build_classifier_prompt(llm_chapter_json))
    return response
