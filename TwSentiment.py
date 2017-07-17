# -*- coding: utf-8 -*-
"""
Created on Fri Apr  8 11:45:26 2016

@author: Alexandre Bovet <alexandre.bovet@gmail.com>

Sentiment Analysis of tweets
"""


from nltk.corpus import stopwords
import collections
from nltk import precision, recall, BigramAssocMeasures
from nltk.probability import FreqDist, ConditionalFreqDist
from nltk import ngrams
from itertools import chain
import numpy as np
from string import punctuation

def bag_of_words(words):
    return dict([(word, True) for word in words])

def bag_of_words_and_bigrams(words):
    
    bigrams = ngrams(words, 2)
    
    return bag_of_words(chain(words, bigrams))    
       
def bag_of_words_not_in_set(words, badwords):
    return bag_of_words(set(words) - set(badwords))
            
def bag_of_words_in_set(words, goodwords):
    return bag_of_words(set(words) & set(goodwords))

def bag_of_non_stopwords(words, stopfile='english'):
    badwords = stopwords.words(stopfile)
    return bag_of_words_not_in_set(words, badwords)    

def bag_of_non_stopwords_and_bigrams(words, stopfile='english'):
    
    words = [w.lower() for w in words]
             
    bigrams = ngrams(words, 2)
    
    badwords = stopwords.words(stopfile)
    punct = [c for c in punctuation]
    
    words = set(words) - set(badwords) - set(punct)
    
    # remove bigrams with punctuation
    bigrams = [b for b in bigrams if b[0] not in punct and b[1] not in punct]

    #remove bigrams where the two words are stop words
    bigrams = [b for b in bigrams if b[0] not in badwords or b[1] not in badwords]            

    return bag_of_words(chain(words, bigrams))    
    
def label_feats_from_corpus(corp, feature_detector=bag_of_words):
    """ returns a featureset for each label in the form {label: [featureset]} """
    label_feats = collections.defaultdict(list)
    for label in corp.categories():
        for fileid in corp.fileids(categories=[label]):
            feats = feature_detector(corp.words(fileids=[fileid]))
            label_feats[label].append(feats)
    return label_feats

def split_label_feats(lfeats, split=0.9):
    """takes a mapping returned from label_feats_from_corpus() and splits
        each list of feature sets into labeled training and testing instances
        preserving the proportions of each labels"""
    
    train_feats = []
    test_feats = []
    for label, feats in lfeats.items():
        cutoff = int(len(feats) * split)
        train_feats.extend([(feat, label) for feat in feats[:cutoff]])
        test_feats.extend([(feat, label) for feat in feats[cutoff:]])
        
    return train_feats, test_feats

def k_fold_generator(labeled_feats, k_fold=10):
    """ a generator that yield train and test sets for k-fold cross validation
        keeping the original proportions of each labels
        
        returns train_feats, test_feats
        """

    train_feats = []
    test_feats = []
    for k in range(k_fold):
        for label, feats in labeled_feats.items():
            
            subset_size = int(len(feats)/k_fold)
            
            train_feats.extend([(feat, label) for feat in feats[:k*subset_size]])
            train_feats.extend([(feat, label) for feat in feats[(k+1)*subset_size:]])
            
            test_feats.extend([(feat, label) for feat in feats[k*subset_size:][:subset_size] ])
        
        yield train_feats, test_feats


def precision_recall(classifier, testfeats):
    """ computes precision and recall of a classifier """
    
    refsets = collections.defaultdict(set)
    testsets = collections.defaultdict(set)
    
    for i, (feats, label) in enumerate(testfeats):
        refsets[label].add(i)
        observed = classifier.classify(feats)
        testsets[observed].add(i)
        
    precisions = {}
    recalls = {}

    for label in classifier.labels():
        precisions[label] = precision(refsets[label], testsets[label])
        recalls[label] = recall(refsets[label], testsets[label])
        
    return precisions, recalls


def high_information_words(labelled_words, 
                           score_fn=BigramAssocMeasures.chi_sq,
                           min_score=5):
    """ returns a set of words with the highest information  """
    
    """
    n_ii : frequency for the word for the label
    n_ix : total freq for the word across all labels
    n_xi : total freq of all words that occured for the label
    n_xx : total freq for all words in all labels
    
    """
    
    
    word_fd = FreqDist()
    label_word_fd = ConditionalFreqDist()
    
    for label, words in labelled_words:
        for word in words:
            word_fd[word] += 1
            label_word_fd[label][word] += 1
            
            
    n_xx = label_word_fd.N()
    high_info_words = set()
    
    for label in label_word_fd.conditions():
        n_xi = label_word_fd[label].N()
        word_scores = collections.defaultdict(int)
    
        for word, n_ii in label_word_fd[label].items():
            n_ix = word_fd[word]
            score = score_fn(n_ii, (n_ix, n_xi), n_xx)
            word_scores[word] = score
            
        bestwords = [word for word, score in word_scores.items() if score >= min_score]
        high_info_words |= set(bestwords)
    
    return high_info_words


#==============================================================================
# Custom Tokenizer for tweets
#==============================================================================

from nltk.tokenize.casual import TweetTokenizer, _replace_html_entities, remove_handles, \
                                reduce_lengthening, HANG_RE, WORD_RE, EMOTICON_RE
import re

def normalize_mentions(text):
    """
    Replace Twitter username handles with '@USER'.
    """
    pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
    return pattern.sub('@USER', text)


def normalize_urls(text):
    """
    Replace urls with 'URL'.
    """  
    pattern = re.compile(r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))""")
    # first shorten consecutive punctuation to 3 
    # to avoid the pattern to hang in exponential loop in extreme cases.    
    text = HANG_RE.sub(r'\1\1\1', text)

    return pattern.sub('URL', text)
    

def _lowerize(word, keep_all_upper=False):
    if EMOTICON_RE.search(word):
        return word
    elif word.isupper() and keep_all_upper:
        return word
    elif word == 'URL':
        return word
    elif word == '@USER':
        return word
    else:
        return word.lower()

class CustomTweetTokenizer(TweetTokenizer):
    """ Custom tweet tokenizer based on NLTK TweetTokenizer"""
    
    def __init__(self, preserve_case=False, reduce_len=True, strip_handles=False, 
                 normalize_usernames=True, normalize_urls=True, keep_allupper=True):
        
        TweetTokenizer.__init__(self, preserve_case=preserve_case, reduce_len=reduce_len, 
                                strip_handles=strip_handles)
                                
        self.keep_allupper = keep_allupper
        self.normalize_urls = normalize_urls
        self.normalize_usernames = normalize_usernames
        
        if normalize_usernames:
            self.strip_handles = False
        
        if self.preserve_case:
            self.keep_allupper = True
        
        
    def tokenize(self, text):
        """
        :param text: str
        :rtype: list(str)
        :return: a tokenized list of strings;
        
        Normalizes URLs, usernames and word lengthening depending of the
        attributes of the instance.
        
        """
        # Fix HTML character entities:
        text = _replace_html_entities(text)
        # Remove or replace username handles
        if self.strip_handles:
            text = remove_handles(text)
        elif self.normalize_usernames:
            text = normalize_mentions(text)
        
        if self.normalize_urls:
            # Shorten problematic sequences of characters
            text = normalize_urls(text)
        
        # Normalize word lengthening
        if self.reduce_len:
            text = HANG_RE.sub(r'\1\1\1', text)
            text = reduce_lengthening(text)
        
        # Tokenize:
        safe_text = HANG_RE.sub(r'\1\1\1', text)
        words = WORD_RE.findall(safe_text)
        
        # Possibly alter the case, but avoid changing emoticons like :D into :d:
        # lower words but keep words that are all upper cases                              
        if not self.preserve_case:
            words = [_lowerize(w, self.keep_allupper) for w in words]
            
            
        return words
        

#==============================================================================
# Direct classification of tweet text
#==============================================================================

def classifyText(text, classifier, tokenizer=CustomTweetTokenizer(),
                  feature_extractor=bag_of_words_and_bigrams,
                  label_inv_mapper={0 : 'neg' , 1 : 'pos'},
                  uncertainty_threshold=0.5,
                  polarity_threshold=0.5):
                      

    labels = [label_inv_mapper[c] for c in  classifier.classes_]


    if isinstance(text, str):
        #single text

        tokens = tokenizer.tokenize(text)
            
        features = feature_extractor(tokens)
        
        proba = classifier.predict_proba(features)
        
        proba = proba.flatten()
        
        if np.abs(proba[0]-proba[1]) > uncertainty_threshold and np.max(proba) > polarity_threshold:
            
            predicted_label = labels[np.argmax(proba)]
        
        else:
            
            predicted_label = 'N/A'
            
         
    elif isinstance(text, list):
        # list of multiple texts

        tokens = map(tokenizer.tokenize, text)
        features = map(feature_extractor, tokens)
        
        proba = classifier.predict_proba(features)
        
        predicted_label = np.zeros(len(text), dtype='<U3')
        predicted_label[:] = 'N/A'
        
        mask = np.all([np.max(proba,axis=1) > polarity_threshold, 
                       np.abs(proba[:,0]-proba[:,1]) > uncertainty_threshold], axis=0)
        
        predicted_label[mask] = [labels[i] for i in np.argmax(proba[mask], axis=1)]


    return predicted_label, proba


#==============================================================================
# Emoticon classification
#==============================================================================

POS_EMOTICONS = [":D", ":-D", ":-)", ":=)", "=)", "XD", "=D", "=]", ":]", ":<)",
                 ":>)", "=}", ":)",":}", ":o)", "(=", "[:", "8D","8-)",
                 "(:", "(-:", ":')", ":-3", ":]", ":-}", ":-]",":-.)","^_^", "^-^"]

NEG_EMOTICONS = [":(", ":-(", ":'(", "=(", "={", "):", ")':", ")=", "}=",
                ":-{", ":-{", ":-(", ":'{", "=[", ":["]
                
POS_EMOJIS_RE = re.compile(u'['
                         u'\U0001F600-\U0001F606'
                         u'\U0001F60A-\U0001F60E'
                         u'\U0001F638-\U0001F63B]+', 
                         re.UNICODE)

NEG_EMOJIS_RE = re.compile(u'['
                        u'\U0001F61E-\U0001F622'
                        u'\U0001F63E-\U0001F63F]+', 
                        re.UNICODE)
                        
def classifyEmoticons(text):
    
    # find all emoticons
    emoticons = EMOTICON_RE.findall(text)
    
    pos = any([emo in POS_EMOTICONS for emo in emoticons]) or bool(POS_EMOJIS_RE.search(text))
    neg = any([emo in NEG_EMOTICONS for emo in emoticons]) or bool(NEG_EMOJIS_RE.search(text))

    if pos and neg:
        return 'N/A'
    elif pos and not neg:
        return 'pos'
    elif neg and not pos:
        return 'neg'
    elif not pos and not neg:
        return None

# list of official Twitter Clients (should be updated regularly)
official_twitter_clients = ['Twitter for iPhone',
 'Twitter for Android',
 'Twitter Web Client',
 'Twitter for iPad',
 'Mobile Web (M5)',
 'TweetDeck',
 'Mobile Web',
 'Mobile Web (M2)',
 'Twitter for Windows',
 'Twitter for Windows Phone',
 'Twitter for BlackBerry',
 'Twitter for Android Tablets',
 'Twitter for Mac',
 'Twitter for BlackBerry®',
 'Twitter Dashboard for iPhone',
 'Twitter for  iPhone',
 'Twitter Ads',
 'Twitter for  Android',
 'Twitter for Apple Watch',
 'Twitter Business Experience',
 'Twitter for Google TV',
 'Chirp (Twitter Chrome extension)',
 'Twitter for Samsung Tablets',
 'Twitter for MediaTek Phones',
 'Google',
 'Facebook',
 'Twitter for Mac',
 'iOS',
 'Instagram',
 'Vine - Make a Scene',
 'Tumblr']
