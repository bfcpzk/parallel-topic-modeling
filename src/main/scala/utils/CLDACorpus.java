package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CLDACorpus {

	private int[][] documents;
	private int vocabularySize = 0;

	public CLDACorpus(InputStream is) throws IOException {

		Set<Integer> vocab = new HashSet<Integer>();
		int length, word, counts;
		List<List<Integer>> docList = new ArrayList<List<Integer>>();
		List<Integer> doc;
		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			try {
				doc = new ArrayList<Integer>();
				String[] fields = line.split(" ");
				//length = Integer.parseInt(fields[0]);
				for (int n = 0; n < fields.length - 1; n++) {
					String[] wordCounts = fields[n + 1].split(":");
					word = Integer.parseInt(wordCounts[0]);
					counts = Integer.parseInt(wordCounts[1]);
					vocab.add(word);
					for (int i = 0; i < counts; i++)
						doc.add(word);
					if (word >= vocabularySize){
						vocabularySize = word + 1;
					}
				}
				docList.add(doc);
			} catch (Exception e) {
				System.err.println(e.getMessage() + "\n");
			}
		}
		System.out.println(vocab.size());
		documents = new int[docList.size()][];
		for (int j = 0; j < docList.size(); j++) {
			doc = docList.get(j);
			documents[j] = new int[doc.size()];
			for (int i = 0; i < doc.size(); i++) {
				documents[j][i] = doc.get(i);
			}
		}
	}

	public int[][] getDocuments() {
		return documents;
	}

	public int getVocabularySize() {
		return vocabularySize;
	}

}
