"""
Minimal mock Tally server for local testing.
Responds to XML requests with fake voucher data.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler

MOCK_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <BODY>
    <IMPORTDATA>
      <REQUESTDATA>
        <TALLYMESSAGE>
          <VOUCHER>
            <GUID>TEST-GUID-001</GUID>
            <VOUCHERNUMBER>V001</VOUCHERNUMBER>
            <VOUCHERTYPE>Payment</VOUCHERTYPE>
            <DATE>20240401</DATE>
            <NARRATION>Test payment for rent</NARRATION>
            <ALLLEDGERENTRIES.LIST>
              <LEDGERNAME>Rent Expense</LEDGERNAME>
              <AMOUNT>-50000.00</AMOUNT>
            </ALLLEDGERENTRIES.LIST>
            <ALLLEDGERENTRIES.LIST>
              <LEDGERNAME>Bank Account</LEDGERNAME>
              <AMOUNT>50000.00</AMOUNT>
            </ALLLEDGERENTRIES.LIST>
          </VOUCHER>
          <VOUCHER>
            <GUID>TEST-GUID-002</GUID>
            <VOUCHERNUMBER>V002</VOUCHERNUMBER>
            <VOUCHERTYPE>Receipt</VOUCHERTYPE>
            <DATE>20240402</DATE>
            <NARRATION>Payment received from customer</NARRATION>
            <ALLLEDGERENTRIES.LIST>
              <LEDGERNAME>Bank Account</LEDGERNAME>
              <AMOUNT>-100000.00</AMOUNT>
            </ALLLEDGERENTRIES.LIST>
            <ALLLEDGERENTRIES.LIST>
              <LEDGERNAME>Sales Account</LEDGERNAME>
              <AMOUNT>100000.00</AMOUNT>
            </ALLLEDGERENTRIES.LIST>
          </VOUCHER>
        </TALLYMESSAGE>
      </REQUESTDATA>
    </IMPORTDATA>
  </BODY>
</ENVELOPE>"""

class MockTallyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Tally is running")

    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(MOCK_RESPONSE.encode("utf-8"))

    def log_message(self, format, *args):
        pass  # suppress default logging

def run(port=9000):
    server = HTTPServer(("localhost", port), MockTallyHandler)
    print(f"Mock Tally server running on port {port}")
    server.serve_forever()

if __name__ == "__main__":
    run()