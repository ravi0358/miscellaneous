import time
def batch_split_df(step1,step2,batch,Dataframe):
    index_values = list(range(step1,step2,batch))
    df_list = []
    for i in range(0,len(index_values)):
        if(i==len(index_values)-1):
            break;
        else:
            print(i)
            range_val1 = index_values[i];
            range_val2 = index_values[i+1];
            start = time.process_time()
            df_new = Dataframe.iloc[range_val1:range_val2,:]
            df_new['cmp_cosine'] = df_new.apply(lambda x: get_cosine(x['company'],x['company_name']),axis=1)
            df_new['cmp_Fuzzy'] = df_new.apply(lambda x:minEdit(x['company'], x['company_name']), axis=1)
            df_new2 = df_new[(df_new['cmp_cosine']>0.7)|
                       (df_new['cmp_Fuzzy']>70)|
                       ((df_new['cmp_cosine']>0.6)&(df_new['cmp_Fuzzy']>60))]
            #df_new2 = df_new[(df_new['cmp_cosine']>0.4)&(df_new['cmp_Fuzzy']>40)]
            df_list.append(df_new2)
            print(df_new.shape)
            print(df_new2.shape)
            df_new2.to_csv('batch_data_'+str(i)+'.csv',index=False)
            #df_new.to_csv('manual_batch_data'+'clean_both'+str(i)+'.csv',index=False)
            print(time.process_time() - start)
    Final_DF = pd.concat(df_list)
    return Final_DF
    
    
start = time.process_time()
df_fourth_final = batch_split_df(0,46000000,2000000,df_fourth)
print(time.process_time() - start)