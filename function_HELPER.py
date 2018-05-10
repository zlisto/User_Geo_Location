from sklearn.linear_model import LogisticRegression
from gmplot import gmplot
from matplotlib import pyplot as plt
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

alambda = 0.98
alpha=[log(5), 0.004, 0.0004] # Only matters if psi is fixed; otherwise alpha will be learned from data



def X(profile):
    return(features(profile,location_terms,wcs,languages,utc_offsets)[0])
    

### Features function based on these lists
### This function returns variable types as well in order to create SQL tables.
def features(profile,loc_terms,wcs_terms,lang_lst,utcoffset_lst):  
    location = profile['location'].lower()
    description = profile['description'].lower()
    screen_name = profile['screen_name'].lower()
    name = profile['name'].lower()
    X=[]
    T=[]
    X.append(1 if any([i.lower() in location for i in wcs_terms]) else 0)
    T.append("BOOL")
    X.append(1 if any([i.lower() in description for i in wcs_terms]) else 0)
    T.append("BOOL")
    X.append(1 if any([i.lower() in name for i in wcs_terms]) else 0)
    T.append("BOOL")
    X.append(1 if any([i.lower() in screen_name for i in wcs_terms]) else 0)
    T.append("BOOL")
    for term in loc_terms:
        X.append(1 if term.lower() in location else 0)
        T.append("BOOL")
        X.append(1 if term.lower() in description else 0)
        T.append("BOOL")
        X.append(1 if term.lower() in name else 0)
        T.append("BOOL")
        X.append(1 if term.lower() in screen_name else 0)
        T.append("BOOL")
    X.append(1 if (location=="" or location is None) else 0)
    T.append("BOOL")
    X.append(1 if profile['lang'] in lang_lst else 0)
    T.append("BOOL")
    X.append(1 if profile['utc_offset'] in utcoffset_lst else 0)
    T.append("BOOL")
    X.append(1 if profile['protected'] else 0)
    T.append("BOOL")
    X.append(1 if profile['verified'] else 0)
    T.append("BOOL")
    return(X,T)

    #### This function takes geo located points from the dataset and fits a logistic regression model on them using the features.
#### The function returns the odds ratio function, which can be used as phi
#### If there are not enough geo-located points, use a fixed phi function instead.


def fit_LR(data_obj,
        radius,
        plot_filename='LogReg_ROC',
        penty='l1',
        l_values=[0.01,0.1,0.5,1,5,10,100]
):
    data_obj.c.execute("SELECT user.user_id,user.ML_set,geo_user.lat,geo_user.lon,features.* FROM user INNER JOIN geo_user ON user.user_id=geo_user.user_id INNER JOIN features ON geo_user.user_id=features.user_id WHERE geo_user.geo_tweet=1;")
    geo_result=data_obj.c.fetchall()
    x_train=[i[5:len(i)] for i in geo_result if i[1]=='TRN']
    y_train=[1 if data_obj.lldist(data_obj.loclatlong,[i[2],i[3]])<=radius else 0 for i in geo_result if i[1]=='TRN']
    x_val=[i[5:len(i)] for i in geo_result if i[1]=='VAL']
    y_val=[1 if data_obj.lldist(data_obj.loclatlong,[i[2],i[3]])<=radius else 0 for i in geo_result if i[1]=='VAL']
    x_test=[i[5:len(i)] for i in geo_result if i[1]=='TST']
    y_test=[1 if data_obj.lldist(data_obj.loclatlong,[i[2],i[3]])<=radius else 0 for i in geo_result if i[1]=='TST']
    if sum(y_train)<3 or sum(y_train)>(len(y_train)-3):
        lambd="default"
        print("Assigning default coefficients because there are not enough points.")
        print("This does not work well.  Try a fixed probability model or get more geo-points.\n")
        lcoefs=len(geo_result[0][5:len(geo_result[0])])
        coefs=[-2]*4+[0.25]*(lcoefs-9)+[0]*5
        intercept=-1
        test_predict=[[1./(exp(intercept+sum([i[j]*coefs[j] for j in range(len(coefs))]))+1),1-1./(exp((intercept+sum([i[j]*coefs[j] for j in range(len(coefs))])))+1)] for i in x_test]
    else:
        best_value=-float('inf')
        for lambd in l_values:
            # lambd=l_values[2]
            LR=LogisticRegression(penalty=penty,C=lambd)
            LR.fit(x_train,y_train)
            val_predict=LR.predict_log_proba(x_val)
            value=sum([val_predict[i][1] if y_val[i] else val_predict[i][0] for i in range(len(y_val))]) # Higher value is better: sum(log(odds))
            if value > best_value:
                best_value=value
                best_lambda=lambd
            print(str(lambd)+": "+str(value))
        lambd=best_lambda
        LR=LogisticRegression(C=lambd,penalty=penty)
        LR.fit(x_train,y_train)
        coefs=LR.coef_[0].tolist()
        intercept=LR.intercept_[0].tolist()
        test_predict=LR.predict_proba(x_test)
    gt=[[y_test[i],test_predict[i][0]] for i in range(len(x_test))]
    s=list(set([i[1] for i in gt]))
    s.sort()
    ins=len([i for i in gt if i[0]])
    outs=len([i for i in gt if not i[0]])
    TP=[0]
    FA=[0]
    auc=0
    finding_p=True
    p_cutoff_x=0
    p_cutoff_y=0
    p_cutoff=0.5
    if ins>0 and outs>0:
        for i in s:
            y0=TP[len(TP)-1]
            x0=FA[len(FA)-1]
            TP.append(len([j for j in gt if j[1]<=i and j[0]])*1./ins)
            FA.append(len([j for j in gt if j[1]<=i and not j[0]])*1./outs)
            y1=TP[len(TP)-1]
            x1=FA[len(FA)-1]
            if finding_p:
                if i >=p_cutoff:
                    p_cutoff_x=x1
                    p_cutoff_y=y1
                    finding_p=False
            auc+=(x1-x0)*(y0+y1)*1./2
        plt.plot(FA,TP)
        plt.scatter(p_cutoff_x,
            p_cutoff_y,
            marker='D',
            c='red',
            s=30)
        plt.plot([0,1],[0,1],'--')
        if data_obj.location_name != "":
            plt.title("Test ROC Plot for "+data_obj.location_name+", LR Model Only")
        else:
            plt.title("Test ROC Plot for LR Model Only")
        plt.text(0.75,0.05,"AUC: {0:1.3f}".format(auc),size=12)
        plt.text(p_cutoff_x,p_cutoff_y,r"$P=${0:1.3f}".format(p_cutoff),size=12)
        plt.xlabel(r"$P_{F}$")
        plt.ylabel(r"$P_{D}$")
        plt.savefig('./'+ plot_filename+ '-testc'+str(lambd)+'.png')
        plt.clf()
        plt.close()
    def phi(x):
        return(intercept+sum([coefs[i]*x[i] for i in range(len(coefs))]))
    return(phi)



def psi(left_right,right_left,u1_features,u2_features):
    #here alpha is a vector of length three, psi decays according to a logistic sigmoid function
    lr_addend_01=0
    rl_addend_01=0
    lr_addend_10=0
    rl_addend_10=0
    lr_addend_00=0
    rl_addend_00=0
    if left_right:
        if u1_features['friends_count']==0 or u2_features['followers_count']==0:
            print("Relationship problem: "+str(u1_features['user_id'])+" --> "+str(u2_features['user_id']))
        fr=max(u1_features['friends_count'],1)
        fo=max(u2_features['followers_count'],1)
        if alpha[1]*fr+alpha[2]*fo < 10: #prevent exp overflow
            lr_addend_01=alpha[0]/(1+exp(-2+alpha[1]*fr+alpha[2]*fo))
        else:
            lr_addend_01=0
        lr_addend_10=lr_addend_01
        lr_addend_00=alambda*(min(lr_addend_01,lr_addend_10))
    if right_left:
        if u1_features['followers_count']==0 or u2_features['friends_count']==0:
            print("Relationship problem: "+str(u1_features['user_id'])+" <-- "+str(u2_features['user_id']))
        fr=max(u2_features['friends_count'],1)
        fo=max(u1_features['followers_count'],1)
        if alpha[1]*fr+alpha[2]*fo < 10: #prevent exp overflow
            rl_addend_01=alpha[0]/(1+exp(-2+alpha[1]*fr+alpha[2]*fo))
        else:
            rl_addend_01=0
        rl_addend_10=rl_addend_01
        rl_addend_00=alambda*(min(rl_addend_01,rl_addend_10))
    return([(lr_addend_01)+(rl_addend_01),(lr_addend_10)+(rl_addend_10),+(lr_addend_00)+(rl_addend_00)])

def readCredentials(path):
    file = open(path, 'r').read().split('\n')
    res = {}
    for line in file:
        temp = line.split(':')
        res[temp[0]] = temp[1:]

    return res;