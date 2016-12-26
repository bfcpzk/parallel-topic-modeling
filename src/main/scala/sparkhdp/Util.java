package sparkhdp;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by zhaokangpan on 2016/12/19.
 */
public class Util {
    public static void swap(int[] arr, int arg1, int arg2){//一维数组交换
        int t = arr[arg1];
        arr[arg1] = arr[arg2];
        arr[arg2] = t;
    }

    public static void swap(int[][] arr, int arg1, int arg2) {//二维数组交换
        int[] t = arr[arg1];
        arr[arg1] = arr[arg2];
        arr[arg2] = t;
    }

    public static double[] ensureCapacity(double[] arr, int min){
        int length = arr.length;
        if (min < length)
            return arr;
        double[] arr2 = new double[min*2];
        for (int i = 0; i < length; i++)
            arr2[i] = arr[i];
        return arr2;
    }

    public static int[] ensureCapacity(int[] arr, int min) {
        int length = arr.length;
        if (min < length)
            return arr;
        int[] arr2 = new int[min*2];
        for (int i = 0; i < length; i++)
            arr2[i] = arr[i];
        return arr2;
    }

    public static int[][] add(int[][] arr, int[] newElement, int index) {
        int length = arr.length;
        if (length <= index){
            int[][] arr2 = new int[index*2][];
            for (int i = 0; i < length; i++)
                arr2[i] = arr[i];
            arr = arr2;
        }
        arr[index] = newElement;
        return arr;
    }

    public static void resultOutput(ShareVar sv, DocState[] docArray, int iter) throws IOException{
        PrintStream file = new PrintStream("coe_" + iter + ".txt");
        for (int k = 0; k < sv.numberOfTopics; k++) {
            for (int w = 0; w < sv.sizeOfVocabulary; w++)
                file.format("%05d ",sv.wordCountByTopicAndTerm[k][w]);
            file.println();
        }
        file.close();


        file = new PrintStream("result_" + iter + ".txt");
        file.println("d w z t");
        int t, docID;
        for (int d = 0; d < docArray.length; d++) {
            DocState docState = docArray[d];
            docID = docState.docId;
            for (int i = 0; i < docState.documentLength; i++) {
                t = docState.words[i].tableAssignment;
                file.println(docID + " " + docState.words[i].termIndex + " " + docState.tableToTopic[t] + " " + t);
            }
        }
        file.close();
    }
}
