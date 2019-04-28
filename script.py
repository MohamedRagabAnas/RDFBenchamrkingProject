import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn import tree
np.random.seed(0)

data = pd.read_csv("adult.data")
data.head()

labels=data.iloc[:,-1]
data2=pd.get_dummies(data.iloc[:,:-1])
data2["income"]=labels


X_train, X_test, y_train, y_test = train_test_split(data.iloc[:,0:-1],data.iloc[:,-1], train_size=80, test_size=20, shuffle=False)
dt = DecisionTreeClassifier()
dt.fit(X_train,y_train)
preds=dt.predict(X_test)
accuracy = dt.score(X_test,y_test)

print("Accuracy= " +str(accuracy))