import json
import subprocess

from utils import try_except_with_log


def run_gemini(prompt: str) -> dict | None:
    current_prompt = prompt
    for attempt in range(2):
        result = subprocess.run(
            # ["gemini", "-p", current_prompt],
            ["gemini", "-p", current_prompt, "-m", "gemini-2.5-flash"],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        if result.returncode != 0:
            print(f"Gemini CLI failed: {result.stderr}")
            return None

        llm_output = result.stdout.strip()

        # Clean up markdown code blocks
        if "```json" in llm_output:
            llm_output = llm_output.split("```json")[1].split("```")[0].strip()
        elif "```" in llm_output:
            # Handle case where it's just wrapped in backticks without json marker
            llm_output = llm_output.split("```")[1].split("```")[0].strip()

        try:
            summary_json = json.loads(llm_output)

            return summary_json

        except json.JSONDecodeError as e:
            # Prepare for retry with corrective prompt
            current_prompt = (
                prompt
                + f"Your previous response had a JSON formatting error: {e}.\n Here is the invalid response you provided:\n\n {llm_output} \n\n Please correct the JSON and provide the full, valid JSON object."
            )

    return None


@try_except_with_log("Sending Gemini request for topic extraction")
def llm_summary(transcribe_json):
    prompt = """
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
    
**Example of Valid Output:**

~~~
{
  "chapters": [
    {
      "id": 0,
      "theme": "Advertising",
      "summary": "Company: Standupclub.ru. The advertisement promotes the website for watching stand-up comedy shows, highlighting the availability of new releases and ad-free viewing."
    },
    {
      "id": 15,
      "theme": "Travel Mishaps and Unexpected Situations",
      "summary": "The comedian recounts a series of travel mishaps, starting with a gig in Pyatigorsk where the venue was changed from a club to a wedding hall, causing confusion for attendees..."
    }
  ]
}
~~~

### Text for Analysis:

"""

    remove_keys = {"start", "end"}
    filter_transcribe_json = {
        key: {k: v for k, v in value.items() if k not in remove_keys}
        for key, value in transcribe_json.items()
    }
    base_prompt = prompt + str(filter_transcribe_json)

    # Get Gemini Respone
    gemini_response = run_gemini(base_prompt)

    # Add end_id for each chapter
    end_id = max(map(int, filter_transcribe_json.keys()))
    for i in gemini_response["chapters"][::-1]:
        i["end_id"] = end_id
        end_id = i["id"] - 1

    return gemini_response


@try_except_with_log("Sending Gemini request for topic classification")
def llm_classifier(llm_chapter_json):
    prompt = """
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

**Example of a valid output:**
~~~json
{
  "classifications": [
    {
      "id": 0,
      "reason": "The summary explicitly describes a promotion for Kozel non-alcoholic beer, which is a food and beverage product.",
      "main_category": "Advertising"
      "subcategory": "Food & beverages",
    },
    {
      "id": 29,
      "reason": "The narrative is centered around a car theft, a police report, the recovery of stolen items, and the eventual capture of the criminals, which directly relates to themes of crime and the justice system.",
      "main_category": "Politics & Society"
      "subcategory": "Law, crime & justice",
    }
  ]
}
~~~

**Summaries Data to Classify:**

"""

    for chapter in llm_chapter_json.get("chapters", []):
        chapter.pop("end_id", None)

    base_prompt = prompt + str(llm_chapter_json)
    return run_gemini(base_prompt)
