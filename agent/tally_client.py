from __future__ import annotations

from typing import Any, Dict, List

import logging
import requests
from lxml import etree


logger = logging.getLogger(__name__)


class TallyClient:
    """
    Thin wrapper around a Tally Desktop instance.
    Only responsibility: talk to Tally's XML HTTP interface,
    parse responses into Python dicts. No business logic here.
    """

    def __init__(self, base_url: str = "http://localhost:9000"):
        self.base_url = base_url.rstrip("/")

    def _post_xml(self, xml_body: str) -> str:
        """
        Low-level helper: POST XML to Tally, return raw XML response.
        """
        logger.debug("Sending XML to Tally: %s", xml_body[:200])
        resp = requests.post(
            self.base_url,
            data=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
            timeout=30,
        )
        resp.raise_for_status()
        logger.debug("Received XML from Tally: %s", resp.text[:200])
        return resp.text

    def health_check(self) -> bool:
        """
        Probe Tally with a real XML request.
        GET doesn't work on Tally — must use POST with valid XML.
        """
        xml_body = """
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Companies</REPORTNAME>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>""".strip()
        try:
            self._post_xml(xml_body)
            return True
        except Exception as exc:
            logger.warning("Tally health check failed: %s", exc)
            return False

    def fetch_raw_ledgers(self) -> List[Dict[str, Any]]:
        """
        Fetch all ledger accounts from Tally.
        Returns list of dicts with GUID, Name, Parent.
        """
        xml_body = """
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Accounts</REPORTNAME>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>""".strip()

        try:
            raw_xml = self._post_xml(xml_body)
            root = etree.fromstring(raw_xml.encode("utf-8"))
        except Exception as exc:
            logger.warning("fetch_raw_ledgers failed: %s", exc)
            return []

        ledgers = []
        for l in root.xpath(".//LEDGER"):
            name_el = l.find("NAME")
            parent_el = l.find("PARENT")
            guid_el = l.find("GUID")
            ledgers.append({
                "GUID": guid_el.text.strip() if guid_el is not None and guid_el.text else "",
                "Name": name_el.text.strip() if name_el is not None and name_el.text else "",
                "Parent": parent_el.text.strip() if parent_el is not None and parent_el.text else "",
            })
        return ledgers

    def get_daybook(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Fetch Daybook vouchers from Tally for the given date range.
        from_date and to_date must be in YYYYMMDD format.
        """
        xml_body = f"""
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Daybook</REPORTNAME>
    <STATICVARIABLES>
      <SVFROMDATE>{from_date}</SVFROMDATE>
      <SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>""".strip()

        raw_xml = self._post_xml(xml_body)
        root = etree.fromstring(raw_xml.encode("utf-8"))

        vouchers: List[Dict[str, Any]] = []

        for v in root.xpath(".//VOUCHER"):
            def text_or_empty(tag_name: str) -> str:
                el = v.find(tag_name)
                return el.text.strip() if el is not None and el.text is not None else ""

            ledger_entries: List[Dict[str, Any]] = []
            for le in v.findall(".//ALLLEDGERENTRIES.LIST"):
                ledger_name_el = le.find("LEDGERNAME")
                amount_el = le.find("AMOUNT")
                ledger_entries.append({
                    "LedgerName": (
                        ledger_name_el.text.strip()
                        if ledger_name_el is not None and ledger_name_el.text
                        else ""
                    ),
                    "Amount": float(amount_el.text) if amount_el is not None and amount_el.text else 0.0,
                })

            voucher_dict: Dict[str, Any] = {
                "GUID": text_or_empty("GUID"),
                "VoucherNumber": text_or_empty("VOUCHERNUMBER"),
                "VoucherType": text_or_empty("VOUCHERTYPE"),
                "Date": text_or_empty("DATE"),
                "Narration": text_or_empty("NARRATION"),
                "LedgerEntries": ledger_entries,
            }
            vouchers.append(voucher_dict)

        return vouchers


__all__ = ["TallyClient"]