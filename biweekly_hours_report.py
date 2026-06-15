from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import io
import os

SMTP_SERVER = 'local28union-com.mail.protection.outlook.com'
SMTP_PORT = 25
EMAIL_FROM = 'szheng@local28union.com'
EMAIL_TO = 'sstarace@local28union.com,jesi@local28union.com,bmontijo@local28union.com'
EMAIL_CC = 'wmills@local28union.com,szheng@local28union.com'
SAVE_FOLDER = '/opt/airflow/reports'

def is_second_monday(**kwargs):
    today = kwargs['logical_date'].date()
    if today.weekday() != 0:
        return False
    return 8 <= today.day <=14

def run_and_email():
    # 1. Run query
    hook = MsSqlHook(mssql_conn_id='mssql_local28')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute  ("""
        with a as
        (select h.personid as [MemberId]
        ,e.ssnlastfour as [SSN Last 4 Digit]
        ,e.nationalidentifier as [IA Number]
        ,e.sortname AS [MemberName]
        ,H.hourssingletime+h.hourshalftime+h.hoursdoubletime as [HoursWorked]
        ,h.date as [WeekEndingDate]
        ,dense_rank () over(partition by h.personid order by h.date desc) as rnk
        from cor_hour h
        left join cor_entity e on e.PERSONid = h.personid
        left join cor_entity com on com.companyid = h.companyid
        left join fin_order o on o.hourid = h.id
        left join rem_report rep on rep.id = o.reportid
        left join rem_request req on req.id = rep.requestid
        where h.dd is null and e.dd is null and com.dd is null and o.dd is null
        and date between '2026-01-01' and getdate()
        and hourssingletime+hourshalftime+hoursdoubletime >0
        and req.Committed is not null
        and H.organizationalunitid is null)
        , b as
        (SELECT ENTITYID,e.NationalIdentifier as [IA Number], statusid,ref.name as status,STARTDATE
        FROM CON_EntityStatusChange ec
        left join cor_reference ref on ref.id = ec.statusid
        left join cor_entity e on e.PersonId = ec.EntityId
        where ec.dd is null --and ec.dm is null 
        and e.dd is null
        and ec.nextid is null and ref.dd is null
        and statusid in ('56320000','-9'))
        select a.[MemberName] as Name,a.[IA number],b.status as Status
        , b.StartDate as [Status StartDate],WeekEndingDate
        , a.HoursWorked as [Latest Recorded Hr]
        , Case when weekendingdate > startdate and WeekEndingDate >= dateadd(day, -30, getdate())
        then 'Marked as Need to Check'
        else ' ' end as [Check or Not]
        from a
        left join b on a.MemberId= b.EntityId
        where a.rnk = 1
        and b.EntityId is not null
        order by a.WeekEndingDate desc
    """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    print(f'Query returned {len(rows)} rows')

    # 2. Build Excel file in memory
    df = pd.DataFrame(rows, columns=columns)
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    
        # Auto-fit all columns
        worksheet = writer.sheets['Report']
        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
            worksheet.column_dimensions[col[0].column_letter].width = max_length + 2

    excel_buffer.seek(0)

    # 3. Build filename e.g. Suspended and Forfeit Report 04292026.xlsx
    today = datetime.today().strftime('%m%d%Y')
    filename = f'Suspended and Forfeit Report {today}.xlsx'

    # 4. Save to folder
    os.makedirs(SAVE_FOLDER, exist_ok=True)
    file_path = f'{SAVE_FOLDER}/{filename}'
    with open(file_path, 'wb') as f:
        f.write(excel_buffer.getvalue())
    print(f'File saved to {file_path}')

    # 5. Reset buffer for email
    excel_buffer.seek(0)

    # 6. Send email
    msg = MIMEMultipart()
    msg['Subject'] = f'Suspended and Forfeit Report - {datetime.today().strftime("%m/%d/%Y")}'
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Cc'] = EMAIL_CC

    
    msg.attach(MIMEText(f'Please find attached the Suspended and Forfeit Report as of {datetime.today().strftime("%m/%d/%Y")}.\n\nThis report is auto-generated every second Monday of the month at 11AM.'))

    attachment = MIMEBase('application', 'octet-stream')
    attachment.set_payload(excel_buffer.read())
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(attachment)
    with smtplib.SMTP(SMTP_SERVER, 25) as s:
        all_recipients = [e.strip() for e in EMAIL_TO.split(',') + EMAIL_CC.split(',')]
        s.sendmail(EMAIL_FROM, all_recipients, msg.as_string())

    print(f'Email sent successfully with attachment: {filename}')

with DAG(
    dag_id='Monthly_Suspended_Forfeit_Report',
    start_date=datetime(2026, 5, 11, 11, 0),
    schedule='0 15 * * 1',
    catchup=False
) as dag:
    
    check_second_monday = ShortCircuitOperator(
        task_id='check_second_monday',
        python_callable=is_second_monday,
    )

    run_query = PythonOperator(
        task_id='run_and_email',
        python_callable=run_and_email,
    )

    check_second_monday >> run_query


    