
# coding: utf-8

# # Problem 1.

# Define spark context and data source

# In[1]:

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
path = "file:///home/jovyan/work/auto_mpg_original.csv"   


# Load the data file and split in into columns and rows. THe file contains 406 records. Also is presented first record's content

# In[2]:

raw_data = sc.textFile(path)
records = raw_data.map(lambda x: x.split(","))  
records.cache()
num_data = records.count()
first = records.first() 
print "First data row: %s" %first 
print "Total number of rows %u" %num_data


# Create test and training datasets. Test will be 20% of the total number of records, training wiil be the other 80%. Test plus train data add up to the original DDR number of rows

# In[3]:

records_with_idx = records.zipWithIndex().map(lambda (k, v): (v, k)) 
test_data_idx = records_with_idx.sample(False, 0.2, 42)
training_data_idx = records_with_idx.subtractByKey(test_data_idx) 

test_data = test_data_idx.map(lambda (idx, p) : p) 
training_data = training_data_idx.map(lambda (idx, p) : p) 
print "Traning data first record %s" %training_data.first()
print "Test data first record %s" %training_data.first()
print "Number of test records: %u" %test_data.count()
print "Number of training records: %u" %training_data.count()
print "Total number of data  rows: %u" %(test_data.count() + training_data.count())


# Linear regression model using the training data converted to vector. To avoid raising error NA values need to be removed since they cannot be converted to float as required by the regression model function

# In[4]:

from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
import numpy as np

test_dt = test_data.filter(lambda v: v[3]<>"NA").map(lambda r: LabeledPoint(float(r[3]),np.array(r[2:3])))
data_dt = training_data.filter(lambda v: v[3]<>"NA").map(lambda r: LabeledPoint(float(r[3]),np.array(r[2:3]))) 

print data_dt.take(5)

linear_model = LinearRegressionWithSGD.train(data_dt, iterations=200,step=0.000001, intercept=False) 
print linear_model


# Use the test data to illustrate accuracy of the linear regression model and its ability to predict the relationship. We compare trained values versus predicted according to the linear regression model

# In[5]:

print test_dt.take(1)
true_vs_predicted = test_dt.map(lambda p: (p.label, linear_model.predict(p.features)))
print "Linear Model predictions: " + str(true_vs_predicted.take(10)) 


# Calculate two standard measures of model accuracy, in this case MSE and MAE

# In[183]:

def squared_error(actual, pred):     
    return (pred - actual)**2

def abs_error(actual, pred):    
    return np.abs(pred - actual) 


mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean() 
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean() 

print "Mean Square Error (MSE): %f" %mse
print "Mean Absolute Error (MAE): %f" %mae


# Plot linear regression model from displacement 0 to 400. Add scaterplot with test values

# In[8]:

import matplotlib.pyplot as plt


x_model=[0, 450]
y_model = [linear_model.intercept, linear_model.intercept + 450 * linear_model.weights[0]]


plt.plot(x_model, y_model, color='red')

print x_model
plt.xlabel('displacement')
plt.ylabel('horsepower')

x_real = test_data.map(lambda v: float(v[2]))
y_real = test_data.map(lambda v: float(v[3]))


plt.plot (x_real.collect(), y_real.collect(), "go")
plt.show()


# # Problem 2.

# Vector lenght calculation for binary encoding

# In[75]:

def get_mapping(rdd, idx):    
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap() 

data_ord = records.filter(lambda v: v[3]<>"NA" and v[0]<>"NA").map(lambda v: (v[1],v[6],v[7],v[8],v[2],v[3],v[4],v[5],v[0]))

print data_ord.take(1)
mappings = [get_mapping(data_ord, i) for i in range(0,4)] 
cat_len = sum(map(len, mappings))
num_len = num_len = len(data_ord.first()[4:8]) 
total_len = num_len + cat_len 

print "Feature vector length for categorical features: %d" % cat_len 
print "Feature vector length for numerical features: %d" % num_len
print "Total feature vector length: %d" % total_len


# In[76]:

def extract_features(record):    
    cat_vec = np.zeros(cat_len)    
    i = 0    
    step = 0    
    for field in record[0:4]:      
        m = mappings[i]       
        idx = m[field]       
        cat_vec[idx + step] = 1       
        i = i + 1       
        step = step + len(m)    
    num_vec = np.array([float(field) for field in record[4:8]])    
    return np.concatenate((cat_vec, num_vec)) 
 
def extract_label(record):     
    return float(record[8]) 

data_bin = data_ord.map(lambda r: LabeledPoint(extract_label(r),extract_features(r)))

print data_bin.take(1)


first_point = data_bin.first() 
print "Label: " + str(first_point.label) 
print "Linear Model feature vector:\n" + str(first_point.features) 
print "Linear Model feature vector length: " + str(len(first_point. features)) 


# Regression model with 8 feature variables and one label variable

# In[187]:

#from the binary encoded DDR, sample 20% for training and rest for test
records_ord_with_idx = data_bin.zipWithIndex().map(lambda (k, v): (v, k)) 
test_data_ord_idx = records_ord_with_idx.sample(False, 0.2, 42)
training_data_ord_idx = records_ord_with_idx.subtractByKey(test_data_ord_idx) 

test_data_ord_bin = test_data_ord_idx.map(lambda (idx, p) : p) 
training_data_ord_bin = training_data_ord_idx.map(lambda (idx, p) : p)

linear_model_bin = LinearRegressionWithSGD.train(training_data_ord_bin, iterations=200,step=0.000001, intercept=False)
print linear_model_bin


# In[104]:

true_vs_predicted_bin = test_data_ord_bin.map(lambda p: (p.label, linear_model_bin.predict(p.features)))
print str(true_vs_predicted_bin.take(5))

mse_bin = true_vs_predicted_bin.map(lambda (t, p): squared_error(t, p)).mean() 
mae_bin = true_vs_predicted_bin.map(lambda (t, p): abs_error(t, p)).mean() 


print "Mean Square Error (MSE): %f" %mse_bin
print "Mean Absolute Error (MAE): %f" %mae_bin


# # Problem 3

# Creater feature vectors for the decision tree

# In[154]:

def get_mapping_dt(rdd, idx):    
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()  

#convert car string variable to numeric
print "Mapping of first categorical feature column: %s" % get_mapping(data_ord, 3) 


def extract_features_dt(record):    
    return np.array(map(float, record[0:7])) 

def extract_label_dt(record):     
    return float(record[7]) 
 
#removed car model from the feauture variables, I could manage to add the dictionary to the RDD
data_tree_no_car_type = data_ord.map(lambda v: (v[0],v[1],v[2],v[4],v[5],v[6],v[7],v[8]))


data_tree = data_tree_no_car_type.map(lambda r: LabeledPoint(extract_label_dt(r), extract_features_dt(r))) 

first_point_tree = data_tree.first() 
print "Decision Tree feature vector: " + str(first_point_tree.features) 
print "Decision Tree feature vector length: " + str(len(first_point_tree.features)) 


# In[167]:

from pyspark.mllib.tree import DecisionTree

#from the RDD sample 20% for training and rest for test
records_tree_with_idx = data_tree.zipWithIndex().map(lambda (k, v): (v, k)) 
test_tree_idx = records_tree_with_idx.sample(False, 0.2, 42)
training_tree_idx = records_tree_with_idx.subtractByKey(test_tree_idx) 

test_tree = test_tree_idx.map(lambda (idx, p) : p) 
training_tree = training_tree_idx.map(lambda (idx, p) : p)

model_tree = DecisionTree.trainRegressor(training_tree,{}) 

preds_tree = model_tree.predict(test_tree.map(lambda p: p.features)) 
actual_tree = test_tree.map(lambda p: p.label) 
true_vs_predicted_tree = actual_tree.zip(preds_tree) 

print "Decision Tree predictions: " + str(true_vs_predicted_tree.take(5)) 
print "Decision Tree depth: " + str(model_tree.depth()) 
print "Decision Tree number of nodes: " + str(model_tree.numNodes()) 


# In[177]:

mse_tree = true_vs_predicted_tree.map(lambda (t, p): squared_error(t, p)).mean() 
mae_tree = true_vs_predicted_tree.map(lambda (t, p): abs_error(t, p)).mean() 


print "Mean Square Error(MSE):  Linear Regression %f" %mse_bin  + "| Decision Tree %f" %mse_tree

print "Mean Absolute Error (MAE):  Linear Regression %f" %mae_bin  + "| Decision Tree %f" %mae_tree

 


# # Problem 4

# Calculate the RMSL error for the first linear regression model calculated in Problem 1

# In[188]:

import math
def squared_log_error(pred, actual):    
    return (np.log(pred + 1) - np.log(actual + 1))**2 

rmsle = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_log_error(t,p)).mean()) 

print rmsle


# Now we recalculate the RMSLE using different parameters to feed the model. Using regularization L2

# In[229]:

def evaluate(train, test, iterations, step, regParam,regType,intercept):
    model = LinearRegressionWithSGD.train(train,iterations,step,regParam=regParam, regType=regType, intercept=intercept)
    tp = test.map(lambda p: (p.label, model.predict(p.features)))
    rmsle = np.sqrt(tp.map(lambda (t, p): squared_log_error(t,p)).mean())
    return rmsle 

train_rmsle = training_data.filter(lambda v: v[3]<>"NA").map(lambda r: LabeledPoint(float(r[3]),np.array(r[2:3])))
test_rmsle = test_data.filter(lambda v: v[3]<>"NA").map(lambda r: LabeledPoint(float(r[3]),np.array(r[2:3])))
 
#number of iterations
params = [1, 5, 10, 20, 50, 100, 500, 1000, 2000, 3000]
metrics = [evaluate(train_rmsle, test_rmsle, param, 0.000001, 0.0, 'l2',False) for param in params]
print params
print metrics  #increase in  the number of iterations decreases the error



# Plot previous results

# In[234]:

plt.plot(params, metrics)
plt.xscale('log')
plt.xlabel('number of iterations')
plt.ylabel('RMSLE')


# It seems from the previous that the model reaches a point where increasing the number of iterations does not improve the model, since the error remains the same.
# 
# Next steps is to add the step value to tue equation as a parameter 

# In[231]:

params_step = [0.0000001, 0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0]
metrics_step = [evaluate(train_rmsle, test_rmsle, 100, param, 0.0, 'l2',False) for param in params_step]
print params_step
print metrics_step


# In[233]:

plt.plot(params_step, metrics_step)
plt.xscale('log')
plt.xlabel('Step size')
plt.ylabel('RMSLE')


# In[ ]:



