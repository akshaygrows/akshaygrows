
from postgres_connect import *
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import pandas as pd

path = '/Users/apple/Documents/tunnel-ssh.cer'
conn = get_conn('Yes',path)

query = '''
select 
    shipping_city
,   hub
,   rider_id
,   rider_name
,   need_improvement
,   starting_fasr
from public.rider_pip
where pip_date = '2023-12-11' and shipping_city = 'Delhi'
'''

def create_pdf(rider_name, need_improvement, output_path):
    c = canvas.Canvas(output_path, pagesize=letter)
    c.setFont("ArialUnicodeMS", 12)
    c.drawString(100, 750, f"प्रिय {rider_name},")
    c.drawString(100, 730, "मुझे आशा है कि आप स्वस्थ हैं। आपका समर्पण मूल्यवान है। पिछले हफ्ते के प्रदर्शन के आधार पर, आपको एक प्रदर्शन सुधार कार्यक्रम में रखा गया है।")
    c.drawString(100, 710, "यह सुधार का एक अवसर है, लेकिन यह भी एक महत्वपूर्ण समय है। आपकी सफलता हमारे लिए अत्यंत महत्वपूर्ण है। इस हफ्ते के अंत तक, आपका प्रदर्शन मॉनिटर किया जाएगा, और इसके परिणामस्वरूप, कंपनी से समाप्ति की संभावना है।")
    c.drawString(100, 690, "हम इस कदम की आवश्यकता के लिए खेद करते हैं, लेकिन समर्थन और सहायता पूरे के पूरे उपलब्ध हैं।")
    c.drawString(100, 670, f"आपके पिछले हफ्ते के प्रदर्शन में सुधार की आवश्यकता है: {need_improvement}")
    c.drawString(100, 650, "लक्ष्य -")
    c.drawString(120, 630, "सुबह: Prepaid FASR 85%, COD FASR 60%")
    c.drawString(120, 610, "शाम: Prepaid FASR 80%, COD FASR 50%")
    c.drawString(100, 590, f"शुभकामनाएं")
    c.save()

def generate_pip_pdfs_by_hub(df_pip):
    hubs = df_pip['hub'].unique()

    for hub in hubs:
        hub_df = df_pip[df_pip['hub'] == hub]
        output_path = f"{hub}_Performance_Improvement_Program.pdf"

        with pd.option_context('mode.chained_assignment', None):
            hub_df.reset_index(drop=True, inplace=True)

        for index, row in hub_df.iterrows():
            rider_name = row['rider_name']
            need_improvement = row['need_improvement']

            create_pdf(rider_name, need_improvement, output_path)

def main():
    df_pip = pd.read_sql(query,conn)
    generate_pip_pdfs_by_hub(df_pip)
    df_pip.close

if __name__ == "__main__":
    main()