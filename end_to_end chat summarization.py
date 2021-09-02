import pyodbc 
import csv
import datetime

con_id='-LufDehXW5cHqmgXyhkC'
#con_id='-MA2t56tmHKmE5uaZKdw'


import json

def extract_and_group(con_id):
    pre_from_id=""
    fields=""
    values=""
    paragraphs=[]
    senders=[]
    sender="empty"
    with open('/data/data/yourdoctorsonline-prod-conversations-export-2.json',encoding='utf-8') as json_file:
        data = json.load(json_file)
    for r1 in data:
        if(r1==con_id):
            print("con_id",r1)
            for r2 in data[r1]:
                pre_from_id=sender
                sender=data[r1][r2]['fromID']
                if pre_from_id!=sender:# if this is a new sender
                        #print("--------------- ",sender," -------------------------")
                        #print(paragraph)
                        paragraph=""
                content=data[r1][r2]['content']
                content=content.rstrip()
                if content[-1] not in ['?','.','!']:
                    content=content+'.'
                if content.find('\n') != -1:  # this snippet aims to fix the issues in the first message sent by patietns which includes several sentences each in one paragraph   
                    arr=content.split('\n')
                    new_content=''
                    for s in arr:
                        if s[-1] not in ['?','.','!']:
                            s=s+'.'
                            s=s[0].upper()+s[1:]
                        new_content=new_content+s+' '
                    content=new_content    
                    # make sure the first letter is uppercase
                content=content[0].upper()+content[1:]
                paragraphs.append(content)
                senders.append(sender)
    grouped_con=""        
    for i in range(len(paragraphs)):
        #print(senders[i][:3],paragraphs[i])
        grouped_con=grouped_con+paragraphs[i]+'\n'
    return grouped_con


def get_onto_name(ref_onto):
    ref_onto=int(ref_onto)
    from xml.dom import minidom
    itemlist = xmldoc.getElementsByTagName('uima.cas.FSArray') # search in all <uima.cas.FSArray> tags
    for i in range(len(itemlist)):
        s=int(itemlist[i].attributes['_id'].value) # get the id of that tag 
        if(s==ref_onto): # check to verify if this is the id of interest?
            itemlist2 = itemlist[i].getElementsByTagName('i') # if yes, itemlist2 becomes the list of all ontology ids


            for ii in itemlist[i].getElementsByTagName('i'):
                ii.normalize(  )



            onto_itemlist = xmldoc.getElementsByTagName('org.apache.ctakes.typesystem.type.refsem.UmlsConcept')
            for i in range(len(onto_itemlist)):
                s=onto_itemlist[i].attributes['_id'].value
                if(s==ii.firstChild.data):
                    code_text = onto_itemlist[i].attributes['preferredText'].value
                    return (code_text)

def load_doc(filename):
    file = open(filename, 'r')
    text = file.read()
    # since in the processed sentence the medication is not a separate sentence, we add a dot before that to make it a sentenve
    text=text.replace(" My current medications are",". My current medications are") 
    file.close()
    return text
    
    
def get_sender(begin,note):
    x=note[0:begin]
    first_sender=note[2:4]
    #print(first_sender)
    if first_sender=='dr':# it should be lower case
        second_sender='Pt'
    else:
        second_sender='Dr' 
        first_sender='Pt' # since the first sender is mostly pt(in lower case), we modify it here to have it in upper case
    newlines = len(x.split('\n'))
    if newlines%2==1:
        return first_sender
    else:
        return second_sender

def get_token(b,e):
    from xml.dom import minidom
    itemlist = xmldoc.getElementsByTagName('org.apache.ctakes.typesystem.type.syntax.WordToken')
    for i in range(len(itemlist)):
        begin=int(itemlist[i].attributes['begin'].value)
        end=int(itemlist[i].attributes['end'].value)
        token=itemlist[i].attributes['normalizedForm'].value
        if begin==b and end==e:
            return token
    
def importance_to_color(x):
    if x<=0.5:
        return '#ffffff'
    elif  x<=0.1:
        return '#e4ffe1'
    elif  x<=2:
        return '#c1ffb9'
    elif  x<=3:
        return '#84ff73'
    elif  x<=4:
        return '#23ff05'
    else:
        return '#1adc00'
    
def remove_names(message): # to remove patient or doctor names from chats to reduce false positives of the Upper_case keywords
    PATT='My name is\s'
    TITLE = r"(?:[A-Z][a-z]*\.\s*)?"
    MIDDLE_I = r"(?:[A-Z][a-z]*\.?\s*)?"
    NAME2 = r"[A-Z][a-z\s?]+"
    people_name=re.findall(PATT+TITLE+MIDDLE_I +NAME2, message)
    for pname in people_name:
        message=message.replace(pname,'My name is xyz')
    return message    
############################################################################################
#    Final summarization by getting the sentence dataframe and cTAKES concept dataframe    #
#    and determining the importance keywords of all types (cTAKES concept, numbers,        #
#    uppercase, and tf-idf and calculating the importance score for each sentence )        #
############################################################################################
def identify_keywords_in_sentences(df_sentences,df_mentions,note):
    df_final = pd.DataFrame(columns = ['sentence_num','sender','cTAKES_Concepts', 'num_of_symptoms','AnatomicalSite','DiseaseDisorder',
                                       'Medication','Procedure','SignSymptom','numbers_cnt','numbers_value','capital_cnt','capital_value', 
                                       'sentence','sentence_begin', 'sentence_end','importance_score'])

    message=""
    for i, sr in df_sentences.iterrows(): # process sentences one-by-one
        cTAKES_Concepts=""
        capital_words=""
        mention_num=0
        AnatomicalSite=[]
        DiseaseDisorder=[]
        Medication=[]
        Procedure=[]
        SignSymptom=[]

        #------------------------------------------ number --------------------
        list_of_numbers = re.findall('[0-9.-/]*[0-9][0-9.-/]*', sr["sentence_cleaned"])

       #------------------------------------------ Upper case --------------------
        sentence_without_names=remove_names(sr["sentence_cleaned"])
        #print(sentence_without_names)
        capital_words=re.findall('[^.]\s([A-Z]\w+)', sentence_without_names)
        for x in capital_words:
            if x.lower() in stopword:
                while x in capital_words: 
                    capital_words.remove(x)
       #------------------------------------------ TF-IDF terms ---------------------
        tf_idf_value=[]
        tf_idf_words=[]
        sentence=sentence_without_names
        words=sentence.lower().split()
        for index, row in df_tfidf_sorted_flitered.iterrows():
            if index not in stopword:# and index not in list_of_numbers:
                if index in words:
                    tf_idf_value.append(round(row['tfidf'],2))   
                    tf_idf_words.append(index)

       #---------------------------------------- find cTAKES concepts in the current sentence ---------------------
        for j, mr in df_mentions.iterrows(): # for each identified cTAKES concepts
            if (mr['begin']>=sr['begin']+sr['newLines'] and mr['end']<=sr['end']+sr['newLines']) and mr['repeated']==0:
                cTAKES_Concepts=cTAKES_Concepts+mr['token_str']+'|'
                mention_num=mention_num+1
                if mr['cat']=='AnatomicalSiteMention':
                    AnatomicalSite.append(mr['token_str'])
                elif mr['cat']=='DiseaseDisorderMention':
                    DiseaseDisorder.append(mr['token_str'])    
                elif mr['cat']=='MedicationMention':
                    Medication.append(mr['token_str'])
                elif mr['cat']=='ProcedureMention':
                    Procedure.append(mr['token_str'])
                elif mr['cat']=='SignSymptomMention':
                    SignSymptom.append(mr['token_str'])
                    
        sender=get_sender(int(sr['begin']),note)   
        importance_score=(len(AnatomicalSite)*3+
                    len(DiseaseDisorder)*3+
                    len(Medication)*3+
                    len(Procedure)*3+
                    len(SignSymptom)*3+
                    len(list_of_numbers)*3+
                    len(capital_words)*0+
                    sum(map(float,tf_idf_value))*10)/(math.log(len(sentence)+1)+1)

        new_row = {'sentence_num':i,'sender':sender,'cTAKES_Concepts':cTAKES_Concepts,'num_of_symptoms':mention_num,'AnatomicalSite':AnatomicalSite,
                   'DiseaseDisorder':DiseaseDisorder,'Medication':Medication,'Procedure':Procedure,'SignSymptom':SignSymptom,
                   'numbers_value':list_of_numbers,'capital_value':capital_words,
                   'tf_idf_word':tf_idf_words,'tf_idf_value':tf_idf_value,'sentence':sr["sentence"],'sentence_begin':int(sr['begin']),'sentence_end':int(sr['end']),'importance_score':importance_score}
       
        df_final = df_final.append(new_row, ignore_index=True)

    return df_final

def generate_html_summary(df_final,filename):
    import webbrowser

    f = open(filename+".html",'w')


    message = """<html>
    <head></head>
    <body><p><b><center>The summary</center></B></p>"""
    #<table border=1 cellpadding=10 cellspacing=0>"""
    pre_sender=""
    for i,row in df_final.iterrows():
        if pre_sender!=row['sender']: # add a new line if the sender changes
            message=message+"<br><br>"+row['sender']+": "
        if row['sender']=='Pt':
            message=message+' '+"<span style='font-style: normal;background-color:"+importance_to_color(row['importance_score'])+"'>"+row['sentence']+"</span>"
        else:
            message=message+' '+"<span style='font-style: italic;background-color:"+importance_to_color(row['importance_score'])+"'>"+row['sentence']+"</span>"
        pre_sender=row['sender']
    message=message+"</html>"


    #message=message_tbl+message        

    f.write(message)
    f.close()
    
def get_cTAKES_concepts(xmldoc):
    df_mentions = pd.DataFrame(columns = ['sender', 'begin', 'end', 'cat', 'token_str', 'token_fn', 'ontology', 'sentence', 'repeated'])
    mention_list = [
            'org.apache.ctakes.typesystem.type.textsem.SignSymptomMention', 
            'org.apache.ctakes.typesystem.type.textsem.AnatomicalSiteMention',
            'org.apache.ctakes.typesystem.type.textsem.ProcedureMention',
            'org.apache.ctakes.typesystem.type.textsem.DiseaseDisorderMention',
            'org.apache.ctakes.typesystem.type.textsem.MedicationMention'] 
    for itm in mention_list:
        words = itm.split('.')
        cat=words[len(words)-1] # get the cat of the mention type found by ctakes
        itemlist = xmldoc.getElementsByTagName(itm) # 
        for i in range(len(itemlist)):
            begin=int(itemlist[i].attributes['begin'].value)
            end=int(itemlist[i].attributes['end'].value)
            try:
                 onto_code=itemlist[i].attributes['_ref_ontologyConceptArr'].value
            except:
                 onto_code=0

            if int(end)<len(note):
                med=note[begin:end]
            #print(get_sender(note[0:begin+4],begin),'\t',cat,'\t',begin,'\t',end,'\t',get_sentence(note[0:end+2],end),'\t',get_token(begin,end),'\t',note[begin:end],'\t',get_onto_name(onto_code))
            new_row = {'sender':get_sender(begin,note), 
                                                  'begin':begin,
                                                  'end':end,
                                                  'cat':words[len(words)-1],
                                                  'token_str':note_without_newline[begin:end],
                                                  'token_fn':get_token(begin,end), 
                                                  'ontology':get_onto_name(onto_code),
                                                  'repeated':0}
            df_mentions = df_mentions.append(new_row, ignore_index=True)
    #----------------------------------------------------------------------- find repeated mentions -----------------  
    for i, r1 in df_mentions.iterrows():
        for j, r2 in df_mentions.iterrows():
            if((r1['begin']==r2['begin'] and r1['end']!=r2['end']) or 
                (r1['end']==r2['end'] and r1['begin']!=r2['begin'])) :
                if(r1['end']-r1['begin']>r2['end']-r2['begin']):
                    r2['repeated']=1
                else:
                    r1['repeated']=1

    df_mentions.sort_values(['begin'])  
    return df_mentions


def get_sentences(note): #convert the note to a set of separate sentences
    df_sentences = pd.DataFrame(columns = ['begin', 'end','newLines','type','sentence'])
    seg = pysbd.Segmenter(language="en", clean=False)
    idx=0
    end=0
    for sentence in seg.segment(note):
        begin=note.find(sentence,end)
        #print(begin,b2)
        end=idx+len(sentence)
        #print(sentence)
        #print(note[b2:end])
        idx=end
        x=note[0:begin]
        newlines = len(x.split('\n'))-1
        sentence_cleaned=re.sub(r'[^A-Za-z0-9 ]+', '', sentence)
        new_row = {'begin':begin,'end':end,'newLines':newlines,'sentence':sentence,'sentence_cleaned':sentence_cleaned}
        df_sentences = df_sentences.append(new_row, ignore_index=True)
    return df_sentences


#########################################
#           The main code               #
#########################################
import re
import math
import pandas as pd
from xml.dom import minidom # for reading xml file
import pysbd # for sentence segmentation
import webbrowser

file_no=4000
stopword=open('/stopwords/stopwords.txt').read().split()
filename="E:\HZ\my_papers\your doctor online\chats_unified_consecutive_msgs_gropued_1-1000_summarized\\"+str(file_no)
con_id='-LufDehXW5cHqmgXyhkC'
note=extract_and_group(con_id)#load_doc(filename+".txt")
# to remove relines and replace it with two underlines to keep the text lengt unchanged
note_without_newline=note.replace('\n', '__')

xmldoc = minidom.parse('/chats_unified_consecutive_msgs_gropued_1-1000/'+str(file_no)+'.txt.xml')
df_mentions=get_cTAKES_concepts(xmldoc)

######################## Load TF-IDF data ################
from scipy import sparse
tf_idf_vector_loaded = sparse.load_npz("/tf_idf_vector.npz")

tfidf_feature_names = []
with open("/feature_names.txt", "r") as f:
  for line in f:
    tfidf_feature_names.append(line.strip())

#---------------------------------get tfidf vector for the current document --------------------
this_document_vector=tf_idf_vector_loaded[file_no] 
df_tfidf = pd.DataFrame(this_document_vector.T.todense(), index=tfidf_feature_names, columns=["tfidf"]) 
df_tfidf_sorted=df_tfidf.sort_values(by=["tfidf"],ascending=False)
#---------------------------------filter out unimportant terms by applying a threshold --------------------
df_tfidf_sorted_flitered=df_tfidf_sorted[df_tfidf_sorted['tfidf']>0.02]    
######################### end tf-idf ########################
df_sentences=get_sentences(note)
df_final=identify_keywords_in_sentences(df_sentences,df_mentions,note)
generate_html_summary(df_final,filename)
webbrowser.open_new_tab(filename+".html")