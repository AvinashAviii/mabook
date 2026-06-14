import json
import logging
from typing import Optional

import google.generativeai as genai
from config import settings

logger = logging.getLogger(__name__)


class GeminiClient:
    """Wrapper for Google Gemini API"""

    def __init__(self):
        if not settings.GEMINI_API_KEY:
            logger.warning(
                "GEMINI_API_KEY not set. AI features will be disabled.")
            self._model = None
            return

        genai.configure(api_key=settings.GEMINI_API_KEY)
        self._model = genai.GenerativeModel(
            model_name=settings.GEMINI_MODEL,
            generation_config=genai.GenerationConfig(
                temperature=settings.GEMINI_TEMPERATURE,
                max_output_tokens=settings.GEMINI_MAX_TOKENS,
                response_mime_type="application/json",
            ),
        )
        logger.info(
            f"Gemini client initialized with model: {settings.GEMINI_MODEL}")

    @property
    def is_available(self) -> bool:
        return self._model is not None

    def generate(self, prompt: str) -> dict:
        """Send prompt to Gemini and parse JSON response"""
        if not self.is_available:
            raise RuntimeError("Gemini API key not configured")

        logger.debug(f"Sending prompt to Gemini ({len(prompt)} chars)")

        try:
            response = self._model.generate_content(prompt)

            # Extract text
            text = response.text.strip()
            logger.debug(f"Gemini response: {text[:500]}...")

            # Parse JSON
            parsed = json.loads(text)
            return parsed

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini JSON response: {e}")
            logger.error(f"Raw response: {text[:1000]}")
            # Try to extract JSON from markdown code blocks
            if "```json" in text:
                json_str = text.split("```json")[1].split("```")[0].strip()
                return json.loads(json_str)
            raise

        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise


# Global singleton
gemini_client = GeminiClient()
