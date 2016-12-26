package sparkhdp;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by zhaokangpan on 2016/12/17.
 */
public class ShareVar implements Serializable{


    public double beta  = 0.5; // default only
    public double gamma = 1.5;
    public double alpha = 1.0;

    public double[] p;
    public double[] f;

    public int[] numberOfTablesByTopic;
    public int[] wordCountByTopic;//nk
    public int[][] wordCountByTopicAndTerm;//nkv

    public int sizeOfVocabulary;
    public int totalNumberOfWords = 0;
    public int numberOfTopics = 1;
    public int totalNumberOfTables;

}
