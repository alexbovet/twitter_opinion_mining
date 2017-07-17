# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 16:30:00 2016

@author: Alexandre Bovet <alexandre.bovet@gmail.com>

Tweet classification
"""

from TwSentiment import CustomTweetTokenizer, bag_of_words_and_bigrams
import numpy as np


class TweetClassifier(object):
    
    def __init__(self, classifier,
                 tokenizer=CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True, 
                                 strip_handles=False,
                                 normalize_usernames=False, 
                                 normalize_urls=False, 
                                 keep_allupper=False),
                  feature_extractor=bag_of_words_and_bigrams,
                  label_inv_mapper={0 : 'neg' , 1 : 'pos'},
                  polarity_threshold=0.5):
        
        self.classifier = classifier
        self.tokenizer = tokenizer
        self.feature_extractor = feature_extractor
        self.label_inv_mapper = label_inv_mapper
        self.polarity_threshold = polarity_threshold
        self.labels = [self.label_inv_mapper[c] for c in  self.classifier.classes_]
    
    def classify_text(self, text, return_pred_labels=True):
                          
    
        if isinstance(text, str):
            #single text
    
            tokens = self.tokenizer.tokenize(text)
                
            features = self.feature_extractor(tokens)
            
            proba = self.classifier.predict_proba(features)
            
            proba = proba.flatten()
            
            if return_pred_labels:
                if np.max(proba) > self.polarity_threshold:
                    
                    predicted_label = self.labels[np.argmax(proba)]
                
                else:
                    
                    predicted_label = 'N/A'
                
             
        elif isinstance(text, list):
            # list of multiple texts
    
            tokens = map(self.tokenizer.tokenize, text)
            features = map(self.feature_extractor, tokens)
            
            proba = self.classifier.predict_proba(features)
            
            if return_pred_labels:
                len_labels = max(len(l) for l in self.labels)
                
                predicted_label = np.zeros(len(text), dtype='<U' + str(len_labels))
                predicted_label[:] = 'N/A'
                
                mask = np.max(proba,axis=1) > self.polarity_threshold
                
                predicted_label[mask] = [self.labels[i] for i in np.argmax(proba[mask], axis=1)]
                            
    
        if return_pred_labels:                            
            return predicted_label, proba
        else:
            return proba