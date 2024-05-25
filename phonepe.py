import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import os
import json


client = mysql.connector.connect(host = "localhost",
                                 user = "root",
                                 password = "Farshi@25",
                                 database = "Phonepe_data")
cursor = client.cursor()


path1= "C:/Users/Abdul/Untitled Folder/project_2/pulse/data/aggregated/transaction/country/india/state/"
aggre_state_list=os.listdir(path1)

col_1={'State':[], 'Year':[], 'Quarter':[], 'Transaction_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

for i in aggre_state_list:
    p_i=path1+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
                Name=z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']

                col_1['Transaction_type'].append(Name)
                col_1['Transacion_count'].append(count)
                col_1['Transacion_amount'].append(amount)
                col_1['State'].append(i)
                col_1['Year'].append(j)
                col_1['Quarter'].append(int(k.strip('.json')))

Aggre_Trans = pd.DataFrame(col_1)

Aggre_Trans['State']=Aggre_Trans['State'].str.replace('andaman-&-nicobar-islands','andaman & nicobar')
Aggre_Trans['State']=Aggre_Trans['State'].str.replace('-',' ')
Aggre_Trans['State']=Aggre_Trans['State'].str.title()
Aggre_Trans['State']=Aggre_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


def Agre_trans_count_amt_y_q(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from aggregated_transaction where Year=%s and Quarter=%s group by State""" ),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_count','Transaction_amount'])
    
    fig_count=px.bar(df,
                x='State',
                y='Transaction_count',
                title=f'QUARTER {quarter} {year} TRANSACTION COUNT',
                height=600)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='State',
                y='Transaction_amount',
                title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT',
                height=650)
    st.plotly_chart(fig_amt)

    fig_chp_count = px.choropleth(df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transaction_count',
    color_continuous_scale='Viridis',
    title=f'QUARTER {quarter} {year} TRANSACTION COUNT')

    fig_chp_count.update_geos(fitbounds="locations", visible=False)
    
    st.plotly_chart(fig_chp_count)

    fig_chp_amount = px.choropleth(df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_amount',
        color_continuous_scale='Viridis',
        title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT')

    fig_chp_amount.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_chp_amount)


def Aggre_trans_count_amt_s(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Transaction_type,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from aggregated_transaction where State=%s and Year=%s and Quarter=%s
                    group by Transaction_type"""),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_type','Transaction_count','Transaction_amount'])

    fig_count=px.pie(data_frame=df,
                    names='Transaction_type',
                    values='Transaction_count',
                    width=600,
                    title=f"{state} TRANSACTION COUNT",
                    color_discrete_sequence=px.colors.sequential.Blugrn_r,
                    hole=0.4)
    st.plotly_chart(fig_count)

    fig_amt=px.pie(data_frame=df,
                    names='Transaction_type',
                    values='Transaction_amount',
                    width=600,
                    title=f"{state} TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Blugrn_r,
                    hole=0.4)
    st.plotly_chart(fig_amt)


def aggre_user_count_y_q(year,quarter):
    cursor.execute(("""select Year,Quarter,Brand,
                    sum(Transacion_count) as Transaction_Count 
                    from aggregated_user where Year=%s and Quarter=%s group by Brand""" ),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['Year','Quarter','Brand','Transaction_count'])

    fig_count=px.bar(df,
                    x='Brand',
                    y='Transaction_count',
                    title=f"QUARTER {quarter} {year} TRANSACTION COUNT VS BRAND",
                    height=600)
    st.plotly_chart(fig_count)


def aggre_user_s(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Brand,
                    sum(Transacion_count) as Transaction_Count,
                    avg(Percentage) as Percentage
                    from aggregated_user where State=%s and Year=%s and Quarter=%s 
                    group by Brand""" ),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Brand','Transaction_count','Percentage'])

    fig_count=px.line(df,
                    x='Brand',
                    y='Transaction_count',
                    hover_data='Percentage',
                    title=f"{state} TRANSACTION COUNT,BRAND,PERCENTAGE",
                    width=1000,
                    markers=True)
    st.plotly_chart(fig_count)


def map_trans_count_amt_y_q(year,quarter):

    cursor.execute(("""select State,Year,Quarter,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from map_transaction where Year=%s and Quarter=%s group by State"""),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_count','Transaction_amount'])

    fig_count=px.bar(df,
                    x='State',
                    y='Transaction_count',
                    title=f'QUARTER {quarter} {year} TRANSACTION COUNT',
                    color_discrete_sequence=px.colors.sequential.Blugrn_r,
                    height=600)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='State',
                y='Transaction_amount',
                title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Blugrn,
                height=600)
    st.plotly_chart(fig_amt)

    fig_chp_count = px.choropleth(df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_count',
        color_continuous_scale='Viridis',
        title=f'QUARTER {quarter} {year} TRANSACTION COUNT')

    fig_chp_count.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_chp_count)

    fig_chp_amount = px.choropleth(df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_amount',
        color_continuous_scale='Viridis',
        title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT')

    fig_chp_amount.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_chp_amount)


def map_trans_count_amt_d(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Districts,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from map_transaction where State=%s and Year=%s and Quarter=%s 
                    group by Districts"""),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Districts','Transaction_count','Transaction_amount'])

    fig_count=px.bar(df,
                x='Transaction_count',
                y='Districts',
                orientation='h',
                title=f'{state} TRANSACTION COUNT',
                color_discrete_sequence=px.colors.sequential.Greys_r)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='Transaction_amount',
                y='Districts',
                orientation='h',
                title=f'{state} TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Greys_r)
    st.plotly_chart(fig_amt)


def map_user_ru_ao_y_q(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Registered_Users) as Registered_Users,
                    sum(AppOpens) as AppOpens 
                    from map_user where Year=%s and Quarter=%s group by State"""),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Registered_Users','AppOpens'])

    fig_1=px.line(df,
                x='State',
                y=['Registered_Users','AppOpens'],
                title=f'QUARTER {quarter} {year} REGISTERED USERS VS APPOPENS',
                height=800,
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                markers=True)
    st.plotly_chart(fig_1)


def map_user_ru_ao_d(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Districts,
                    sum(Registered_Users) as Registered_Users,
                    sum(AppOpens) as AppOpens 
                    from map_user where State=%s and Year=%s and Quarter=%s 
                    group by Districts"""),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Districts','Registered_Users','AppOpens'])

    fig_1=px.bar(df,
                x='Registered_Users',
                y='Districts',
                orientation='h',
                title=f'{state}  REGISTERED USERS',
                color_discrete_sequence=px.colors.sequential.Darkmint_r)
    st.plotly_chart(fig_1)
    
    fig_2=px.bar(df,
                x='AppOpens',
                y='Districts',
                orientation='h',
                title=f'{state}  APPOPENS',
                color_discrete_sequence=px.colors.sequential.Darkmint_r)
    st.plotly_chart(fig_2)


def top_trans_count_amt_y_q(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Transaction_count) as Transaction_Count,
                    sum(Transaction_amount) as Transaction_Amount 
                    from top_transaction where Year=%s and Quarter=%s group by State"""),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_count','Transaction_amount'])

    fig_count=px.bar(df,
                    x='State',
                    y='Transaction_count',
                    title=f'QUARTER {quarter} {year} TRANSACTION COUNT',
                    color_discrete_sequence=px.colors.sequential.Magenta_r,
                    height=600)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='State',
                y='Transaction_amount',
                title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Magenta_r,
                height=600)
    st.plotly_chart(fig_amt)

    fig_chp_count = px.choropleth(df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_count',
        color_continuous_scale='Viridis',
        title=f'QUARTER {quarter} {year} TRANSACTION COUNT')

    fig_chp_count.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_chp_count)

    fig_chp_amount = px.choropleth(df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_amount',
        color_continuous_scale='Viridis',
        title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT')

    fig_chp_amount.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig_chp_amount)


def top_trans_count_amt_pc(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Pincodes,
                    sum(Transaction_count) as Transaction_Count,
                    sum(Transaction_amount) as Transaction_Amount 
                    from Top_transaction where State=%s and Year=%s and Quarter=%s 
                    group by Pincodes"""),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Pincodes','Transaction_count','Transaction_amount'])

    fig_1=px.scatter(df,
                x='Pincodes',
                y='Transaction_count',
                title=f'{state} TRANSACTION COUNT',
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                height=600)
    st.plotly_chart(fig_1)


def top_user_ru_y_q(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Registered_Users) as Registered_Users
                    from Top_User where Year=%s and Quarter=%s group by State"""),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Registered_Users'])
    
    fig_1=px.line(df,
                x='State',
                y='Registered_Users',
                title=f'QUARTER {quarter} {year} REGISTERED USERS ',
                height=800,
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                markers=True)
    st.plotly_chart(fig_1)


def top_user_ru_pc(state,year,quarter):
    cursor.execute(("""select State,Year,Quarter,Pincodes,
                    sum(Registered_Users) as Registered_Users
                    from Top_User where State=%s and Year=%s and Quarter=%s 
                    group by Pincodes"""),(state,year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Pincodes','Registered_Users'])

    fig_1=px.scatter(df,
                x='Pincodes',
                y='Registered_Users',
                title=f'{state} REGISTERED USERS',
                color_discrete_sequence=px.colors.sequential.Magenta_r,
                height=600)
    st.plotly_chart(fig_1)


def chart_1(table_name):
    cursor.execute(f"""select State, sum(Transacion_amount) as Transaction_Amount
                        from {table_name} group by State order by Transaction_Amount desc limit 10""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df1 = pd.DataFrame(rows, columns=['State','Transaction_amount'])  

    cursor.execute(f"""select State, sum(Transacion_count) as Transaction_count 
                        from {table_name} group by State order by Transaction_count desc limit 10""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df2 = pd.DataFrame(rows, columns=['State','Transaction_count'])


    fig_1=px.bar(df1,
                x='State',
                y='Transaction_amount',
                title='TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                height=650)
    st.plotly_chart(fig_1)


    fig_2=px.bar(df2,
                x='State',
                y='Transaction_count',
                title='TRANSACTION COUNT',
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                height=650)
    st.plotly_chart(fig_2)


def chart_2(table_name):
    cursor.execute(f"""select State, sum(Transaction_amount) as Transaction_Amount 
                        from {table_name} group by State order by Transaction_Amount desc limit 10""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df1 = pd.DataFrame(rows, columns=['State','Transaction_amount'])  

    cursor.execute(f"""select State, sum(Transaction_count) as Transaction_count 
                        from {table_name} group by State order by Transaction_count desc limit 10""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df2 = pd.DataFrame(rows, columns=['State','Transaction_count'])


    fig_1=px.bar(df1,
                x='State',
                y='Transaction_amount',
                title='TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Burgyl_r,
                height=650)
    st.plotly_chart(fig_1)

    fig_2=px.bar(df2,
                x='State',
                y='Transaction_count',
                title='TRANSACTION COUNT',
                color_discrete_sequence=px.colors.sequential.Burgyl_r,
                height=650)
    st.plotly_chart(fig_2)
        

def chart_3(state):
    cursor.execute(("""select State,Brand,
                        sum(Transacion_count) as Transaction_count 
                        from aggregated_user where State=%s group by Brand 
                        order by Transaction_count""" ),(state,))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Brand','Transaction_count'])

    fig_count=px.line(df,
                    x='Brand',
                    y='Transaction_count',
                    title=f"{state} TRANSACTION COUNT VS BRAND",
                    width=1000,
                    markers=True)
    st.plotly_chart(fig_count)


def chart_4(state):
    cursor.execute(("""select State,Transaction_type,
                        sum(Transacion_count) as Transaction_Count,
                        sum(Transacion_amount) as Transaction_Amount 
                        from aggregated_transaction where State=%s group by Transaction_type"""),(state,))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Transaction_type','Transaction_count','Transaction_amount'])

    fig_count=px.bar(df,
                    y='Transaction_type',
                    x='Transaction_count',
                    width=1000,
                    title="TRANSACTION COUNT",
                    orientation='h',
                    color_discrete_sequence=px.colors.sequential.Blugrn_r)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(data_frame=df,
                    y='Transaction_type',
                    x='Transaction_amount',
                    width=1000,
                    title="TRANSACTION AMOUNT",
                    orientation='h',
                    color_discrete_sequence=px.colors.sequential.Blugrn_r)
    st.plotly_chart(fig_amt)


def chart_5(table_name):
    cursor.execute(f"""select State, sum(Registered_Users) as Registered_Users
                    from {table_name} group by State order by Registered_Users""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df1 = pd.DataFrame(rows, columns=['State','Registered_Users'])

    cursor.execute(f"""select State, sum(AppOpens) as AppOpens
                    from {table_name} group by State order by AppOpens""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df2 = pd.DataFrame(rows, columns=['State','AppOpens'])

    fig_1=px.line(df1,
                x='State',
                y='Registered_Users',
                title= 'REGISTERED USERS',
                height=800,
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                markers=True)
    st.plotly_chart(fig_1)

    fig_2=px.line(df2,
                x='State',
                y='AppOpens',
                title= 'APP USERS',
                height=800,
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                markers=True)
    st.plotly_chart(fig_2)


def chart_6(state):    
    cursor.execute(("""select State,Districts,
                        sum(Registered_Users) as Registered_Users
                        from map_user where State=%s 
                        group by Districts order by Registered_Users"""),(state,))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df1 = pd.DataFrame(rows, columns=['State','Districts','Registered_Users'])

    cursor.execute(("""select State,Districts,
                        sum(AppOpens) as AppOpens
                        from map_user where State=%s 
                        group by Districts order by AppOpens"""),(state,))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df2 = pd.DataFrame(rows, columns=['State','Districts','AppOpens'])


    fig_1=px.bar(df1,
                x='Districts',
                y='Registered_Users',
                title=f'{state} REGISTERED USERS DISTRICTWISE',
                color_discrete_sequence=px.colors.sequential.Darkmint_r)
    st.plotly_chart(fig_1)

    fig_2=px.bar(df2,
                x='Districts',
                y='AppOpens',
                title=f'{state} APP USERS DISTRICTWISE',
                color_discrete_sequence=px.colors.sequential.Darkmint_r)
    st.plotly_chart(fig_2)


def chart_7(table_name):
    cursor.execute(f"""select State, sum(Registered_Users) as Registered_Users
                    from {table_name} group by State order by Registered_Users""")
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Registered_Users'])
    
    fig_1=px.line(df,
                x='State',
                y='Registered_Users',
                title='REGISTERED USERS',
                height=800,
                color_discrete_sequence=px.colors.sequential.Bluered_r,
                markers=True)
    st.plotly_chart(fig_1)


def chart_8(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from aggregated_transaction where Year=%s and Quarter=%s group by State""" ),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_count','Transaction_amount'])
    
    fig_count=px.bar(df,
                x='State',
                y='Transaction_count',
                title=f'QUARTER {quarter} {year} TRANSACTION COUNT',
                height=600)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='State',
                y='Transaction_amount',
                title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT',
                height=650)
    st.plotly_chart(fig_amt)


def chart_9(year,quarter):
    cursor.execute(("""select State,Year,Quarter,
                    sum(Transacion_count) as Transaction_Count,
                    sum(Transacion_amount) as Transaction_Amount 
                    from map_transaction where Year=%s and Quarter=%s group by State"""),(year,quarter))
    rows=cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=['State','Year','Quarter','Transaction_count','Transaction_amount'])

    fig_count=px.bar(df,
                    x='State',
                    y='Transaction_count',
                    title=f'QUARTER {quarter} {year} TRANSACTION COUNT',
                    color_discrete_sequence=px.colors.sequential.Blugrn_r,
                    height=600)
    st.plotly_chart(fig_count)

    fig_amt=px.bar(df,
                x='State',
                y='Transaction_amount',
                title=f'QUARTER {quarter} {year} TRANSACTION AMOUNT',
                color_discrete_sequence=px.colors.sequential.Blugrn,
                height=600)
    st.plotly_chart(fig_amt)



Y_list=[2018,2019,2020,2021,2022,2023,2024]
Q_list=[1,2,3,4]
combined_options = [f'{opt1} , {opt2}' for opt1 in Y_list for opt2 in Q_list]

st.set_page_config(layout="centered")
st.header(":violet[PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION]",divider='rainbow')

page1,page2 = st.tabs([":blue[DATA ANALYSIS]",":blue[TOP CHARTS]"])

with page1:    
    options=st.selectbox("choose an option:",["Aggregated","Map","Top"])

    if options == "Aggregated":
        type=st.radio("Select a type:",["Transaction","User"])

        if type == "Transaction":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            Agre_trans_count_amt_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            Aggre_trans_count_amt_s(state,year,quarter)

        elif type == "User":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            aggre_user_count_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            aggre_user_s(state,year,quarter)

    elif options == "Map":
        type=st.radio("Select a type:",["Transaction","User"])

        if type == "Transaction":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            map_trans_count_amt_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            map_trans_count_amt_d(state,year,quarter)


        elif type == "User":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            map_user_ru_ao_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            map_user_ru_ao_d(state,year,quarter)


    elif options == "Top":
        type=st.radio("Select a type:",["Transaction","User"])

        if type == "Transaction":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            top_trans_count_amt_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            top_trans_count_amt_pc(state,year,quarter)


        elif type == "User":
            selected=st.selectbox('Select year and quarter:',combined_options)
            year,quarter = selected.split(' , ')
            year = int(year)
            quarter = int(quarter)

            top_user_ru_y_q(year,quarter)

            state = st.selectbox('Select a state:',Aggre_Trans['State'].unique())

            top_user_ru_pc(state,year,quarter)


with page2:
    Questions=st.selectbox("Select a question",("1.Transaction count and amount of aggregated transaction",
                                                "2.Transaction count and amount of map transaction",
                                                "3.Transaction count and amount of top transaction",
                                                "4.Brandwise transaction count",
                                                "5.Categories wise transaction count and amount",
                                                "6.Statewise Reg users, App users in map user",
                                                "7.District wise Reg users, App users in map user",
                                                "8.Statewise Reg users in top user",
                                                "9.Aggregated transaction based on year and quarter",
                                                "10.Map transaction based on year and quarter"))


    if Questions == "1.Transaction count and amount of aggregated transaction":
        chart_1(table_name='aggregated_transaction')

    elif Questions == "2.Transaction count and amount of map transaction":
        chart_1(table_name='map_transaction')

    elif Questions == "3.Transaction count and amount of top transaction":
        chart_2(table_name='Top_transaction')

    elif Questions == "4.Brandwise transaction count":
        state = st.radio('Select a state:',Aggre_Trans['State'].unique())
        chart_3(state=state)

    elif Questions == "5.Categories wise transaction count and amount":
        state = st.radio('Select a state:',Aggre_Trans['State'].unique())
        chart_4(state=state)

    elif Questions == "6.Statewise Reg users, App users in map user":
        chart_5(table_name='map_user')

    elif Questions == "7.District wise Reg users, App users in map user":
        state = st.radio('Select a state:',Aggre_Trans['State'].unique())
        chart_6(state=state)

    elif Questions == "8.Statewise Reg users in top user":
        chart_7(table_name='Top_User')

    elif Questions == "9.Aggregated transaction based on year and quarter":
        selected=st.select_slider('Select year and quarter:',combined_options)
        year,quarter = selected.split(' , ')
        year = int(year)
        quarter = int(quarter)

        chart_8(year=year,quarter=quarter)

    elif Questions == "10.Map transaction based on year and quarter":
        selected=st.select_slider('Select year and quarter:',combined_options)
        year,quarter = selected.split(' , ')
        year = int(year)
        quarter = int(quarter)

        chart_9(year=year,quarter=quarter)


