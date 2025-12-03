"""
Tests for technology scoring and blacklist logic.

These tests verify that:
1. get_highest_value_tech correctly skips blacklisted technologies
2. The function falls back to next highest-scoring tech
3. Edge cases are handled properly
"""

import pytest

from stackscanner.tech_scorer import (
    get_highest_value_tech,
    score_technologies,
    TECH_BLACKLIST,
    TECH_SCORES,
)


class TestTechBlacklist:
    """Test that blacklisted technologies are skipped."""

    def test_magento_is_in_blacklist(self):
        """Verify Magento is in the blacklist."""
        assert "Magento" in TECH_BLACKLIST

    def test_skips_magento_returns_next_highest(self):
        """Test that Magento is skipped and next highest-scoring tech is returned."""
        # Magento has score 5, Shopify has score 4
        technologies = ["Magento", "Shopify", "WordPress"]
        result = get_highest_value_tech(technologies)

        assert result is not None
        assert result.name == "Shopify"
        assert result.name != "Magento"

    def test_magento_only_returns_none(self):
        """Test that if only Magento is detected, None is returned."""
        technologies = ["Magento"]
        result = get_highest_value_tech(technologies)

        assert result is None

    def test_magento_with_same_score_techs(self):
        """Test Magento is skipped when other high-score techs exist."""
        # All score 5: Salesforce, Marketo, HubSpot, Segment, Magento, Pardot, Optimizely
        technologies = ["Magento", "Salesforce", "HubSpot"]
        result = get_highest_value_tech(technologies)

        assert result is not None
        assert result.name != "Magento"
        assert result.name in ["Salesforce", "HubSpot"]

    def test_no_blacklisted_tech_works_normally(self):
        """Test normal behavior when no blacklisted tech is present."""
        technologies = ["Shopify", "WordPress", "Google Analytics"]
        result = get_highest_value_tech(technologies)

        assert result is not None
        assert result.name == "Shopify"  # Highest score (4)

    def test_empty_list_returns_none(self):
        """Test that empty list returns None."""
        result = get_highest_value_tech([])

        assert result is None

    def test_all_blacklisted_returns_none(self):
        """Test that if all techs are blacklisted, None is returned."""
        # Only blacklisted techs
        technologies = list(TECH_BLACKLIST)
        result = get_highest_value_tech(technologies)

        assert result is None

    def test_priority_order_maintained(self):
        """Test that priority order is maintained when skipping blacklisted tech."""
        # Magento (5) > Shopify (4) > WordPress (3) > Cloudflare (2) > GA (1)
        technologies = ["Google Analytics", "Cloudflare", "WordPress", "Shopify", "Magento"]
        result = get_highest_value_tech(technologies)

        assert result is not None
        assert result.name == "Shopify"
        assert result.score == 4


class TestScoreTechnologies:
    """Test the score_technologies function still works correctly."""

    def test_scores_all_technologies_including_blacklisted(self):
        """Ensure score_technologies still includes blacklisted techs."""
        technologies = ["Magento", "Shopify", "WordPress"]
        scored = score_technologies(technologies)

        names = [t.name for t in scored]
        assert "Magento" in names
        assert "Shopify" in names
        assert "WordPress" in names

    def test_sorted_by_score_descending(self):
        """Test that technologies are sorted by score in descending order."""
        technologies = ["WordPress", "Shopify", "Magento", "Google Analytics"]
        scored = score_technologies(technologies)

        scores = [t.score for t in scored]
        assert scores == sorted(scores, reverse=True)

    def test_magento_still_has_high_score(self):
        """Verify Magento still has a high score for detection purposes."""
        assert "Magento" in TECH_SCORES
        assert TECH_SCORES["Magento"] == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
