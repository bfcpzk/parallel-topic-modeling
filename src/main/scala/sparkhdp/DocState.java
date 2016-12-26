package sparkhdp;

import java.io.Serializable;

/**
 * Created by zhaokangpan on 2016/12/17.
 */
public class DocState implements Serializable {

    int docId, documentLength, numberOfTables;
    int[] tableToTopic;
    int[] wordCountByTable;
    WordState[] words;

    public DocState(int[] instance, int docId) {
        this.docId = docId;
        this.numberOfTables = 0;
        this.documentLength = instance.length;
        words = new WordState[documentLength];
        wordCountByTable = new int[2];
        tableToTopic = new int[2];
        for (int position = 0; position < documentLength; position++)
            words[position] = new WordState(instance[position], -1);
    }

    public void defragment(int[] kOldToKNew) {
        int[] tOldToTNew = new int[numberOfTables];
        int t, newNumberOfTables = 0;
        for (t = 0; t < numberOfTables; t++){
            if (wordCountByTable[t] > 0){
                tOldToTNew[t] = newNumberOfTables;
                tableToTopic[newNumberOfTables] = kOldToKNew[tableToTopic[t]];
                swap(wordCountByTable, newNumberOfTables, t);
                newNumberOfTables ++;
            } else
                tableToTopic[t] = -1;
        }
        numberOfTables = newNumberOfTables;
        for (int i = 0; i < documentLength; i++)
            words[i].tableAssignment = tOldToTNew[words[i].tableAssignment];
    }

    public static void swap(int[] arr, int arg1, int arg2){//一维数组交换
        int t = arr[arg1];
        arr[arg1] = arr[arg2];
        arr[arg2] = t;
    }

    class WordState implements Serializable{
        int termIndex;//单词序号
        int tableAssignment;//该单词被分配的桌子的编号

        public WordState(int wordIndex, int tableAssignment){
            this.termIndex = wordIndex;
            this.tableAssignment = tableAssignment;
        }
    }
}
