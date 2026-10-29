"""A small help center for the ticketing product (fictional facts), plus labelled questions.
Each question names the article and a unique 'fact' string that must be in the retrieved text to answer it."""

ARTICLES = [
    ("billing-refunds", "Refunds and cancellations", False,
     "You can cancel a subscription at any time from Settings > Billing. Monthly plans stop at the end of the current billing period and are not refunded. "
     "Annual plans can be refunded within 45 days of purchase. After 45 days, the remaining months are converted to account credit instead. "
     "Refunds are issued to the original payment method and take 5 to 10 business days to appear. Taxes are refunded together with the plan price."),
    ("billing-refunds-2023", "Refunds and cancellations (2023)", True,
     "This article describes the policy that applied until December 2023. Annual plans could be refunded within 30 days of purchase. "
     "Monthly plans were refunded pro rata for unused days. Refunds took up to 14 business days."),
    ("billing-invoices", "Invoices and receipts", False,
     "Invoices are generated on the first day of each billing period and emailed to the billing contact. "
     "You can download past invoices as PDF from Settings > Billing > Invoices. To add a VAT number, edit the company profile before the invoice is generated; "
     "invoices that were already issued cannot be changed. Account owners and billing admins can see invoices."),
    ("plans-seats", "Plans and agent seats", False,
     "The Starter plan includes 3 agent seats and costs $49 per month. Additional seats cost $19 per month each. "
     "The Pro plan includes 10 seats and adds automations, SLA policies and the API. "
     "Enterprise includes unlimited light agents, sandbox environments and a dedicated success manager. Seat changes are prorated to the day."),
    ("sla-policies", "SLA policies", False,
     "SLA policies set targets for first response and resolution time per priority. "
     "On the Enterprise plan, urgent tickets receive a first response within 30 minutes, 24/7. High priority tickets on Enterprise get a first response within 2 hours. "
     "On Pro, SLA timers pause outside business hours unless the policy is marked 24/7. "
     "Breached tickets are flagged red in the queue and can trigger an escalation automation."),
    ("uptime", "Uptime commitment", False,
     "The Enterprise SLA commits to 99.95% monthly uptime, excluding scheduled maintenance announced 72 hours in advance. "
     "If uptime falls below the commitment, Enterprise customers receive a service credit of 10% of the monthly fee for each full 0.1% below. "
     "Current status and incident history are published on the status page."),
    ("data-retention", "Data retention and deletion", False,
     "Deleted tickets stay in the trash for 21 days and are then permanently removed. "
     "Call recordings are kept for 90 days unless the account enables extended retention. "
     "When an account is closed, all data is deleted within 30 days, and backups expire within a further 35 days."),
    ("backups", "Backups and disaster recovery", False,
     "Customer data is backed up every 6 hours and backups are kept for 35 days. "
     "Backups are encrypted and stored in a second region. The recovery point objective is 6 hours and the recovery time objective is 4 hours."),
    ("data-residency", "Data residency", False,
     "EU customers' data is hosted in the Frankfurt region. US customers are hosted in Virginia. "
     "Data residency is chosen when the account is created; moving an existing account to another region requires a migration request to support."),
    ("attachments", "Attachments", False,
     "Attachments can be up to 40 MB each. Customers can attach up to 10 files per reply. "
     "Images are shown inline; other files are linked. Attachments are scanned for malware before agents can open them."),
    ("api-limits", "API rate limits", False,
     "The Pro plan allows 600 API requests per minute per account. Enterprise allows 2,500 requests per minute. "
     "When the limit is exceeded, the API returns HTTP 429 with a Retry-After header. "
     "Bulk endpoints count as one request per call, regardless of how many tickets they contain."),
    ("api-errors", "API error codes", False,
     "Error E1042 means the request is missing the X-Account header. Error E2210 means the ticket is locked by an ongoing merge; retry after a few seconds. "
     "Error E3007 is returned when a custom field value does not match the field type. All errors include a request ID for support."),
    ("api-errors-2", "More API error codes", False,
     "Error E1043 means the X-Account header refers to a closed account. Error E1044 means the API key was revoked. "
     "Error E2211 means the ticket was merged into another ticket; use the ID returned in the response. Error E2212 means the ticket is archived and read-only. "
     "Error E3008 means a required custom field is missing. Error E3009 means a dropdown field received a value that is not one of its options."),
    ("webhook-errors", "Webhook delivery errors", False,
     "Webhook error W4101 means your endpoint returned a 5xx status; deliveries are retried for 24 hours. "
     "Error W4102 means the endpoint timed out after 10 seconds. Error W4103 means TLS verification failed for your endpoint certificate. "
     "Error W4104 means the endpoint returned 410 Gone, so the webhook is disabled automatically."),
    ("sso", "Single sign-on", False,
     "SAML single sign-on is available on Pro and Enterprise. Supported identity providers include Okta, Azure AD and Google Workspace. "
     "When SSO is enforced, password login is disabled for agents, but account owners keep a recovery login. "
     "Just-in-time provisioning creates agent accounts on first login."),
    ("two-factor", "Two-factor authentication", False,
     "Two-factor authentication can be required for all agents from Settings > Security. Agents can use an authenticator app or a security key. "
     "If an agent loses their device, an administrator can reset their two-factor setup; the agent must then enroll again at next login."),
    ("trial", "Free trial", False,
     "The free trial lasts 10 days and includes all Pro features. No credit card is required to start. "
     "At the end of the trial, the account switches to read-only until a plan is chosen. Trial data is kept for 30 days after the trial ends."),
    ("custom-fields", "Custom fields", False,
     "Each ticket form can have up to 75 custom fields. Field types include text, number, date, dropdown and checkbox. "
     "Renaming a field keeps its existing values. Deleting a field removes its values from all tickets after a 7-day grace period."),
    ("macros", "Macros", False,
     "Macros apply a set of actions to a ticket in one click: set status, add tags, assign, or insert a reply template. "
     "Personal macros are visible only to their creator; shared macros can be limited to groups. A ticket can have at most 5 macros applied per update."),
    ("email-forwarding", "Email forwarding", False,
     "To receive email as tickets, forward your support address to your account's inbound address. "
     "Forwarding must be verified with a test email before it becomes active. Emails larger than 25 MB in total are rejected and the sender receives a bounce."),
]

QUESTIONS = [
    ("How long do I have to get a refund on a yearly subscription?", "billing-refunds", "within 45 days", "paraphrase"),
    ("What happens to my annual plan after the refund window?", "billing-refunds", "converted to account credit", "span"),
    ("How long until a refund shows up on my card?", "billing-refunds", "5 to 10 business days", "paraphrase"),
    ("Are monthly subscriptions refunded when I cancel?", "billing-refunds", "are not refunded", "paraphrase"),
    ("Can I add our VAT number to an invoice that was already sent?", "billing-invoices", "cannot be changed", "span"),
    ("Where do I download old invoices?", "billing-invoices", "Settings > Billing > Invoices", "paraphrase"),
    ("How many agents are included in Starter?", "plans-seats", "3 agent seats", "paraphrase"),
    ("How much is an extra seat?", "plans-seats", "$19 per month", "paraphrase"),
    ("Which plan gives me a sandbox?", "plans-seats", "sandbox environments", "paraphrase"),
    ("How fast must we answer urgent tickets on Enterprise?", "sla-policies", "within 30 minutes", "paraphrase"),
    ("First response target for high priority on Enterprise?", "sla-policies", "within 2 hours", "keyword"),
    ("Do SLA timers keep running at night on the Pro plan?", "sla-policies", "pause outside business hours", "paraphrase"),
    ("What uptime do you guarantee?", "uptime", "99.95%", "paraphrase"),
    ("What compensation do we get if you miss the uptime target?", "uptime", "service credit of 10%", "paraphrase"),
    ("How long are deleted tickets recoverable?", "data-retention", "21 days", "paraphrase"),
    ("How long do you keep phone call recordings?", "data-retention", "90 days", "paraphrase"),
    ("If we close our account, when is our data gone?", "data-retention", "deleted within 30 days", "paraphrase"),
    ("How frequently is our data backed up?", "backups", "every 6 hours", "paraphrase"),
    ("What is your RTO?", "backups", "recovery time objective is 4 hours", "keyword"),
    ("Where is data stored for European customers?", "data-residency", "Frankfurt", "paraphrase"),
    ("Can we move our account to the EU region later?", "data-residency", "requires a migration request", "paraphrase"),
    ("What's the biggest file a customer can attach?", "attachments", "up to 40 MB", "paraphrase"),
    ("How many files can be attached to one reply?", "attachments", "up to 10 files", "paraphrase"),
    ("What's the API limit on Pro?", "api-limits", "600 API requests per minute", "keyword"),
    ("What do I get back when I send too many API calls?", "api-limits", "HTTP 429", "paraphrase"),
    ("What does error E1042 mean?", "api-errors", "missing the X-Account header", "keyword"),
    ("I get E2210 when updating a ticket, why?", "api-errors", "locked by an ongoing merge", "keyword"),
    ("What is E3007?", "api-errors", "does not match the field type", "keyword"),
    ("What does E1043 mean?", "api-errors-2", "closed account", "keyword"),
    ("Error E1044?", "api-errors-2", "API key was revoked", "keyword"),
    ("Getting E2211 on a ticket update", "api-errors-2", "merged into another ticket", "keyword"),
    ("What is error E3009?", "api-errors-2", "not one of its options", "keyword"),
    ("Webhook failing with W4102", "webhook-errors", "timed out after 10 seconds", "keyword"),
    ("What does W4104 mean for my webhook?", "webhook-errors", "disabled automatically", "keyword"),
    ("W4103 error on webhook deliveries", "webhook-errors", "TLS verification failed", "keyword"),
    ("Does SAML work with Okta?", "sso", "Okta, Azure AD and Google Workspace", "keyword"),
    ("If we enforce SSO, can the owner still log in with a password?", "sso", "keep a recovery login", "span"),
    ("An agent lost their phone with the 2FA app, what now?", "two-factor", "reset their two-factor setup", "paraphrase"),
    ("Do I need a credit card for the trial?", "trial", "No credit card is required", "paraphrase"),
    ("How long is the trial?", "trial", "10 days", "paraphrase"),
    ("What happens to my account when the trial ends?", "trial", "switches to read-only", "paraphrase"),
    ("How many custom fields can a form have?", "custom-fields", "up to 75 custom fields", "paraphrase"),
    ("If I delete a custom field, is the data gone immediately?", "custom-fields", "7-day grace period", "span"),
    ("Can I hide my macros from other agents?", "macros", "visible only to their creator", "paraphrase"),
    ("What is the maximum size of an incoming email?", "email-forwarding", "25 MB", "paraphrase"),
    ("Why aren't forwarded emails creating tickets yet?", "email-forwarding", "must be verified with a test email", "paraphrase"),
    ("How long is the refund window for annual plans?", "billing-refunds", "within 45 days", "keyword"),
]
