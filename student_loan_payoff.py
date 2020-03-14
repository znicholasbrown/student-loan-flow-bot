# General
from datetime import timedelta, timezone, datetime, time
from operator import itemgetter
from numbers import Number
import pprint

# Prefect
import prefect
from prefect import Flow, Task, Parameter
from prefect.client.secrets import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.schedules.filters import between_times
from prefect.environments.storage import Docker

# Google
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# Twilio
from twilio.rest import Client


class CreateAuthDict(Task):
    def run(self, auth_config):
        return dict(
            type=auth_config["type"],
            project_id=auth_config["project_id"],
            client_id=auth_config["client_id"].get(),
            private_key=auth_config["private_key"].get().encode("utf-8"),
            private_key_id=auth_config["private_key_id"].get(),
            client_email=auth_config["client_email"].get(),
            client_x509_cert_url=auth_config["client_x509_cert_url"].get(),
            auth_provider_x509_cert_url=auth_config["auth_provider_x509_cert_url"],
        )


class Authenticate(Task):
    def run(self, auth_config, scopes):
        return ServiceAccountCredentials.from_json_keyfile_dict(auth_config, scopes)


class GetSheet(Task):
    def run(self, creds, spreadsheet_id):
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        return (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range="A1:E14")
            .execute()["values"][1:]
        )


class CreateDictFromRows(Task):
    def run(self, rows):
        loan_dict = dict()

        for row in rows:
            loan_dict[row[0]] = dict(
                original_total=row[1],
                current_total=row[2],
                interest_rate=row[3],
                min_payment=row[4],
            )
        return loan_dict


class OrderLoansOnInterestRate(Task):
    def run(self, rows):
        return sorted(rows, key=itemgetter(3), reverse=True)


class CalcTotalAfterMinPayments(Task):
    def run(self, loans, budget):
        remaining_budget = budget
        for loan in loans:
            remaining_budget -= float(
                loans[loan]["min_payment"].replace("$", "").replace(",", "")
            )

        return round(remaining_budget, 2)


class TargetLoans(Task):
    def run(self, loans, budget):
        current_budget = budget
        payments = dict()

        while current_budget > 0:
            for loan in loans:
                outstanding = float(loan[2].replace("$", "").replace(",", ""))
                if outstanding > 0:
                    amount = (
                        outstanding
                        if current_budget - outstanding >= 0
                        else current_budget
                    )

                    if amount > 0:
                        payments[loan[0]] = amount
                        current_budget -= amount

        return payments


class SendMessage(Task):
    def toFloat(self, val):
        if isinstance(val, Number):
            return val

        return float(val.replace("$", "").replace(",", ""))

    def fv(self, val):
        floated = round(self.toFloat(val), 2)
        return f"{floated:,}"

    def run(
        self,
        account_id,
        auth_token,
        twilio_number,
        phone_number,
        payments,
        current_total,
        last_month_total,
    ):
        current_total = self.toFloat(current_total)
        last_month_total = self.toFloat(last_month_total)

        is_down = current_total < last_month_total
        is_same = current_total == last_month_total

        formatted_current = self.fv(current_total)

        formatted_down = self.fv(last_month_total - current_total)
        formatted_up = self.fv(current_total - last_month_total)

        down_or_up = (
            f"down {formatted_down}🤩 from"
            if is_down
            else "the same 😓 as"
            if is_same
            else f"up {formatted_up} 😵 from"
        )

        message_body = f"""
Hi, it's your student loan bot!🤖

Your current loan balance is ${formatted_current}, {down_or_up} last month.

Here's which loans you should pay this month:

            """

        for payment in payments:
            if payment["payment"] == 0:
                continue
            else:
                load_id = payment["loan_id"]
                amount = payment["payment"]
                in_full = (
                    "🔥 FINISHED 🔥"
                    if self.toFloat(payment["current_total"]) - amount == 0
                    else ""
                )
                formatted_amount = self.fv(amount)
                message_body += f"\n    { load_id }: ${ formatted_amount }"

        message_body += (
            "\n\nDon't forget to update the spreadsheet, and have a GREAT month!"
        )

        client = Client(account_id.get(), auth_token.get())

        message = client.messages.create(
            body=message_body, from_=twilio_number.get(), to=phone_number
        )


class GetLastMonthTotal(Task):
    def run(self, creds, spreadsheet_id):
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        return (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range="B21:B21")
            .execute()["values"][0][0]
        )


class UpdateLastMonthTotal(Task):
    def run(self, creds, spreadsheet_id, total):
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        updated_total = [[total]]
        body = {"values": updated_total}

        return sheet.values().update(
            spreadsheetId=spreadsheet_id, range="B21:B21", body=body
        )


class GetCurrentTotal(Task):
    def run(self, creds, spreadsheet_id):
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        return (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range="B20:B20")
            .execute()["values"][0][0]
        )


class CalcTotalPaymentsForMonth(Task):
    def run(self, min_loans, targeted_loans):
        total_payments = min_loans
        for loan in min_loans:
            total_payments[loan]["payment"] = round(
                float(
                    total_payments[loan]["min_payment"]
                    .replace("$", "")
                    .replace(",", "")
                )
                + (targeted_loans[loan] if loan in targeted_loans else 0),
                2,
            )

        return total_payments


class MakeSortedPaymentList(Task):
    def run(self, payments):
        payment_list = []
        for payment in payments:
            payment_temp = payments[payment]
            payment_temp["loan_id"] = payment
            payment_list.append(payment_temp)

        return sorted(payment_list, key=itemgetter("payment"), reverse=True)


class LogResult(Task):
    def run(self, res):
        return self.logger.info(pprint.pprint(res))


schedule = Schedule(
    clocks=[IntervalClock(interval=timedelta(weeks=1), start_date=datetime.utcnow())]
)
with Flow(name="Loan Payoff Reminder", schedule=schedule) as flow:
    budget = Parameter("budget", default=3000)
    phone_number = Parameter("phone_number", default="+15707306535")

    # Google Parameters
    scopes = Parameter(
        "SCOPES", default=["https://www.googleapis.com/auth/spreadsheets"]
    )
    spreadsheet_id = Parameter(
        "SPREADHSEET_ID", default="1Wb5Anty3nvaa0jHY7NEYz7i6jyIOQ5Q9z2GLqmLjmPA"
    )
    project_id = Parameter("project_id", default="flows-270323")
    auth_provider_x509_cert_url = Parameter(
        "auth_provider_x509_cert_url",
        default="https://www.googleapis.com/oauth2/v1/certs",
    )

    # Google Secrets
    client_id = Secret("GOOGLE_CLIENT_ID")
    private_key = Secret("GOOGLE_PRIVATE_KEY")
    private_key_id = Secret("GOOGLE_PRIVATE_KEY_ID")
    client_email = Secret("GOOGLE_CLIENT_EMAIL")
    client_x509_cert_url = Secret("GOOGLE_CLIENT_CERT_URL")

    # Twilio Secrets
    twilio_account_id = Secret("TWILIO_ACCOUNT_ID")
    twilio_auth_token = Secret("TWILIO_AUTH_TOKEN")
    twilio_number = Secret("TWILIO_NUMBER")

    auth_config = dict(
        type="service_account",
        project_id=project_id,
        client_id=client_id,
        private_key=private_key,
        private_key_id=private_key_id,
        client_email=client_email,
        client_x509_cert_url=client_x509_cert_url,
        auth_provider_x509_cert_url=auth_provider_x509_cert_url,
    )
    create_auth_dict = CreateAuthDict()(auth_config=auth_config)

    authenticate = Authenticate()(create_auth_dict, scopes=scopes)

    get_sheet = GetSheet()(creds=authenticate, spreadsheet_id=spreadsheet_id)

    sorted_rows = OrderLoansOnInterestRate()(get_sheet)
    row_dict = CreateDictFromRows()(get_sheet)

    calc_total_after_min_payments = CalcTotalAfterMinPayments()(
        budget=budget, loans=row_dict
    )

    targeted_loans = TargetLoans()(
        loans=sorted_rows, budget=calc_total_after_min_payments
    )

    total_payments = CalcTotalPaymentsForMonth()(
        min_loans=row_dict, targeted_loans=targeted_loans
    )

    payment_list = MakeSortedPaymentList()(payments=total_payments)

    current_total = GetCurrentTotal()(creds=authenticate, spreadsheet_id=spreadsheet_id)
    last_month_total = GetLastMonthTotal()(
        creds=authenticate, spreadsheet_id=spreadsheet_id
    )

    UpdateLastMonthTotal()(
        creds=authenticate,
        spreadsheet_id=spreadsheet_id,
        upstream_tasks=[last_month_total],
        total=current_total,
    )

    SendMessage()(
        account_id=twilio_account_id,
        auth_token=twilio_auth_token,
        twilio_number=twilio_number,
        phone_number=phone_number,
        payments=payment_list,
        current_total=current_total,
        last_month_total=last_month_total,
    )

flow.storage = Docker(
    base_image="python:3.7",
    python_dependencies=[
        "oauth2client",
        "google-api-python-client",
        "gspread",
        "twilio",
    ],
    registry_url="znicholasbrown",
    image_name="loan-payoff-reminder",
    image_tag="loan-payoff-reminder",
)


flow.register(project_name="Finances")
# flow.run()
