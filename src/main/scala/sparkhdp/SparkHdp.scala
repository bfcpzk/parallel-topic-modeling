package sparkhdp

import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zhaokangpan on 2016/12/17.
  */
object SparkHdp {

  // 启动spark集群
  def startSpark(remote: Boolean) = {
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkHdp").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  /**
    * Decide at which topic the table should be assigned to
    *
    * return the index of the topic
    */
  def sampleTopic(sv: ShareVar) = {
    var pSum = 0.0
    var stop = 0
    var flag = true
    sv.p = Util.ensureCapacity(sv.p, sv.numberOfTopics)
    for(k <- 0 until sv.numberOfTopics){
      pSum += sv.numberOfTablesByTopic(k) * sv.f(k)
      sv.p(k) = pSum
    }
    pSum += sv.gamma / sv.sizeOfVocabulary
    sv.p(sv.numberOfTopics) = pSum
    val u = Random.nextDouble() * pSum
    for(k <- 0 until sv.numberOfTopics + 1){
      if(u < sv.p(k) && flag) {
        stop = k
        flag = false
      }
    }
    stop
  }

  /**
    * Decide at which table the word should be assigned to
    *
    * param docID the index of the document of the current word
    * param i the index of the current word
    * return the index of the table
    */
  def sampleTable(sv : ShareVar, docState : DocState, i : Int) = {
    var flag = true
    var stop = 0
    var pSum,fNew,u = 0.0
    //println("vocabSize : " + sv.sizeOfVocabulary)
    val vb = sv.sizeOfVocabulary * sv.beta
   /* println("vb : " + vb)
    println("f : " + sv.f.length)
    println("numberOfTopics : " + sv.numberOfTopics)*/
    sv.f = Util.ensureCapacity(sv.f, sv.numberOfTopics)
    sv.p = Util.ensureCapacity(sv.p, docState.numberOfTables)
    fNew = sv.gamma / sv.sizeOfVocabulary
    for(k <- 0 until sv.numberOfTopics){
      sv.f(k) = (sv.wordCountByTopicAndTerm(k)(docState.words(i).termIndex) + sv.beta)/
        (sv.wordCountByTopic(k) + vb)
      fNew += sv.numberOfTablesByTopic(k) * sv.f(k)
    }
    for(j <- 0 until docState.numberOfTables){
      if(docState.wordCountByTable(j) > 0){
        pSum += docState.wordCountByTable(j) * sv.f(docState.tableToTopic(j))
      }
      sv.p(j) = pSum
    }
    pSum += sv.alpha * fNew / (sv.totalNumberOfTables + sv.gamma)// Probability for t = tNew
    sv.p(docState.numberOfTables) = pSum
    u = Random.nextDouble() * pSum
    for(j <- 0 until docState.numberOfTables + 1){
      if(u < sv.p(j) && flag){// decided which table the word i is assigned to
        stop = j
        flag = false
      }
    }
    stop
  }


  /*def swapOne(arr : Array[Int], arg1 : Int, arg2 : Int) : Unit = {//一维数组交换
    val t = arr(arg1)
    arr(arg1) = arr(arg2)
    arr(arg2) = t
  }

  def swapTwo(arr : Array[Array[Int]], arg1 : Int, arg2 : Int) : Unit = {//二维数组交换
    val t = arr(arg1)
    arr(arg1) = arr(arg2)
    arr(arg2) = t
  }

  def add(arr : Array[Array[Int]], newElement : Array[Int], index : Int) : Array[Array[Int]] = {
    val len = arr.length
    val arr2 = new Array[Array[Int]](index * 2)
    if (len <= index) {
      for(i <- 0 until len){
        arr2(i) = arr(i)
      }
    }
    arr2(index) = newElement
    arr2
  }

  def ensureCapacityInt(arr : Array[Int], min : Int) : Array[Int] = {
    val len = arr.length
    if (min < len) arr
    val arr2 = new Array[Int](min * 2)
    for(i <- 0 until len){
      arr2(i) = arr(i)
    }
    arr2
  }

  def ensureCapacityDouble(arr : Array[Double], min : Int) : Array[Double] = {
    val len = arr.length
    if (min < len) arr
    val arr2 = new Array[Double](min * 2)
    for(i <- 0 until len){
      arr2(i) = arr(i)
    }
    arr2
  }*/

  def setDoc(line : String) = {
    val termSplit = line.split(" ")
    val buffer = new ArrayBuffer[Int]
    for(i <- 1 until termSplit.length){
      val term = termSplit(i).split(":")
      for(j <- 0 until term(1).toInt){
        buffer.+=(term(0).toInt)
      }
    }
    val docState = new DocState(buffer.toArray, termSplit(0).toInt)
    docState
  }

  /**
    * Removes a word from the bookkeeping
    *
    * param docID the id of the document the word belongs to
    * param i the index of the word
    */
  def removeWord(docState: DocState, sv : ShareVar , i : Int): Unit = {
    val table = docState.words(i).tableAssignment
    val k = docState.tableToTopic(table)
    docState.wordCountByTable(table) -= 1
    sv.wordCountByTopic(k) -= 1
    sv.wordCountByTopicAndTerm(k)(docState.words(i).termIndex) -= 1
    if(docState.wordCountByTable(table) == 0){// table is removed
      sv.totalNumberOfTables -= 1
      sv.numberOfTablesByTopic(k) -= 1
      docState.tableToTopic(table) -= 1
    }
  }

  def calSubSizeOfVocabulary(docArray : Array[DocState], sv : ShareVar) : Unit = {
    val wordSet = new util.HashSet[Int]()
    for(doc <- docArray){
      for(term <- doc.words){
        wordSet.add(term.termIndex)
        if (term.termIndex >= sv.sizeOfVocabulary) {
          sv.sizeOfVocabulary = term.termIndex + 1
        }
      }
    }
  }

  def addInstances(docArray : Array[DocState], sv : ShareVar) : Unit = {
    var wordCount = 0
    for(docState <- docArray){
      wordCount += docState.words.length
    }
    sv.totalNumberOfWords = wordCount
    println("vocabSize : " + sv.sizeOfVocabulary)
    calSubSizeOfVocabulary(docArray, sv)
    println("vocabSize : " + sv.sizeOfVocabulary)
    sv.p = new Array[Double](7)
    sv.f = new Array[Double](7)
    sv.numberOfTablesByTopic = new Array[Int](sv.numberOfTopics + 1)
    sv.wordCountByTopic = new Array[Int](sv.numberOfTopics + 1)
    sv.wordCountByTopicAndTerm = new Array[Array[Int]](sv.numberOfTopics + 1)
    for(k <- 0 until (sv.numberOfTopics + 1)){//var initialization done
      sv.wordCountByTopicAndTerm(k) = new Array[Int](sv.sizeOfVocabulary)
    }
    //println("numberOfTopic : " + sv.numberOfTopics)
    for(k <- 0 until sv.numberOfTopics){
      println("k : " + k)
      for(i <- 0 until docArray(k).documentLength){
        addWord(docArray(k), sv, i, 0, k)
      }
    }
    for(j <- sv.numberOfTopics until docArray.length){
      val kk = Random.nextInt(sv.numberOfTopics + 1)
      println("instance kk : " + kk)
      for(i <- 0 until docArray(j).documentLength){
        //println("i : " + i + " kk : " + kk)
        addWord(docArray(j), sv, i, 0, kk)
      }
      println("instance numberOfTopic : " + sv.numberOfTopics)
    }
    // the words in the remaining documents are now assigned too
    println("numberOfTopics = " + sv.numberOfTopics)
    println("numberOfTables = " + sv.totalNumberOfTables)
  }

  def addWord(doc : DocState, sv: ShareVar, i : Int, table : Int, k : Int) : Unit = {
    val docState = doc
    docState.words(i).tableAssignment = table
    docState.wordCountByTable(table) += 1
    sv.wordCountByTopic(k) += 1
    sv.wordCountByTopicAndTerm(k)(docState.words(i).termIndex) += 1
    if (docState.wordCountByTable(table) == 1) {// a new table is created
      //println("addWord totalNumberOfTables: " + sv.totalNumberOfTables)
      docState.numberOfTables += 1
      docState.tableToTopic(table) = k
      sv.totalNumberOfTables += 1
      sv.numberOfTablesByTopic(k) += 1
      docState.tableToTopic = Util.ensureCapacity(docState.tableToTopic, docState.numberOfTables)
      docState.wordCountByTable = Util.ensureCapacity(docState.wordCountByTable, docState.numberOfTables)
      //println("addWord numberOfTopics: " + sv.numberOfTopics + " k : " + k)
      if (sv.numberOfTopics == k) { // a new topic is created
        //println("addWord numberOfTopics: " + sv.numberOfTopics)
        sv.numberOfTopics += 1
        sv.numberOfTablesByTopic = Util.ensureCapacity(sv.numberOfTablesByTopic, sv.numberOfTopics)
        sv.wordCountByTopic = Util.ensureCapacity(sv.wordCountByTopic, sv.numberOfTopics)
        sv.wordCountByTopicAndTerm = Util.add(sv.wordCountByTopicAndTerm, new Array[Int](sv.sizeOfVocabulary), sv.numberOfTopics)
      }
    }
  }

  /**
    * Step one step ahead
    *
    */
  def nextGibbsSweep(docArray : Array[DocState], sv : ShareVar): Unit = {
    var table = 0
    for(docState <- docArray){
      for(i <- 0 until docState.documentLength) {
        removeWord(docState, sv, i)
        table = sampleTable(sv, docState, i)
        if(table == docState.numberOfTables){
          addWord(docState, sv, i, table, sampleTopic(sv))
        }else{
          addWord(docState, sv, i, table, docState.tableToTopic(table))
        }
      }
    }
    defragment(docArray, sv)
  }

  /**
    * Removes topics from the bookkeeping that have no words assigned to
    */
  def defragment(docArray : Array[DocState], sv : ShareVar): Unit ={
    val kOldToNew = new Array[Int](sv.numberOfTopics)
    var newNumberOfTopics = 0
    for(k <- 0 until sv.numberOfTopics){
      if(sv.wordCountByTopic(k) > 0){
        kOldToNew(k) = newNumberOfTopics
        Util.swap(sv.wordCountByTopic, newNumberOfTopics, k)
        Util.swap(sv.numberOfTablesByTopic, newNumberOfTopics, k)
        Util.swap(sv.wordCountByTopicAndTerm, newNumberOfTopics, k)
        newNumberOfTopics += 1
      }
    }
    sv.numberOfTopics = newNumberOfTopics
    for(doc <- docArray){
      doc.defragment(kOldToNew)
    }
  }

  //程序入口
  def main(args : Array[String]): Unit ={
    val sc = startSpark(false)

    //设置最大迭代次数
    val maxIter = 100

    //数据分块数
    val splitRate = 3

    //变更数据结构
    val data = ArrayBuffer[(Array[DocState], ShareVar)]()

    //文件处理
    val docs = sc.textFile("sampledata.txt").map(l => {
      setDoc(l)
    }).collect

    for(i <- 0 until docs.length){
      docs(i).docId = i
    }

    //sc.stop()

    println("docs: " + docs.length)

    var tempBuffer = ArrayBuffer[DocState]()


    println("rate: " + docs.length/splitRate)

    for(i <- 1 until (docs.length + 1)){
      if(i % (docs.length/splitRate) == 0){//数据封装
        println("数据封装")
        data.+=((tempBuffer.toArray, new ShareVar))
        tempBuffer = ArrayBuffer[DocState]()
        tempBuffer.+=(docs(i - 1))
      }else{//追加数据
        tempBuffer.+=(docs(i - 1))
      }
    }

    println("dataSize : " + data.size)
    
    //并行化数据模块
    val docRDD = sc.parallelize(data.toList).coalesce(1).cache()

    docRDD.collect().foreach(l => println("length : " + l._1.length))

    //HDP处理
    val result = docRDD.map(docBlock =>{
      val docArray = docBlock._1
      val shareVar = docBlock._2
      //初始化文本集参数
      addInstances(docArray, shareVar)
      //训练
      for(iter <- 0 until maxIter){
        nextGibbsSweep(docArray, shareVar)
      }
      (docArray, shareVar)
    }).cache()

    //不同模块训练结果整合
    val resultArray = result.collect

    //对结果打印输出
    for(i <- 0 until resultArray.length){
      val docArray = resultArray(i)._1
      val sv = resultArray(i)._2
      Util.resultOutput(sv, docArray, i)
    }
  }
}
