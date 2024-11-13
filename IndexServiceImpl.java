package edu.dlut.searching.service.impl;

import edu.dlut.searching.model.Answer;
import edu.dlut.searching.model.IndexToken;
import edu.dlut.searching.model.Question;
import edu.dlut.searching.service.AnswerService;
import edu.dlut.searching.service.IndexService;
import edu.dlut.searching.service.QuestionService;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class IndexServiceImpl implements IndexService {
    String IDX_QUESTION = System.getProperty("user.dir") + "\\index\\QuestionIndex";
    String IDX_ANSWER = System.getProperty("user.dir") + "\\index\\AnswerIndex";
    @Autowired
    QuestionService questionService;
    @Autowired
    AnswerService answerService;
    /**
     * 创建lucene的索引
     * segment(段，片)，可以包含多个doc
     * doc--document-一条记录，包含多个field
     * field(域)--相当于字段，包含多个term
     * analyzer--分词器，比如：StandardAnalyzer，把域的内容按自己的规则进行分词
     * term(词语,经过分词器处理后的单元)，比如"休闲","休"，“闲”
     * .si:segment info
     * .cfs,cfe:compound 复合索引文件
     * .fnm,fdt,fdx:field
     * .tim,tip:term
     */
    @Override

    public boolean createQuestionIndex() {
        List<Question> questions = questionService.queryQuestions(0, 10000);
        //Question question=questions.get(0);
        //String string= JSON.toJSONString(question);
        //System.out.println(string);
        //question=JSON.parseObject(string,Question.class);

        FSDirectory directory = null;
        
        directory = FSDirectory.open(Paths.get(IDX_QUESTION));  // 获取索引路径
        IKAnalyzer analyzer = new IKAnalyzer(true);     // 创建分词器
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);   // 把分词器加入索引设置
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);    // 开始新索引时删除之前创建的索引
        // 创建indexwriter用来写索引
        try(IndexWriter writer = new IndexWriter(directory,writerConfig)) {
            for (Question content : questions){
                Long questionId = content.getQuestionId();
                //System.out.println(questionId);
                String title = content.getTitle();
                //System.out.println(title);
                Document document = new Document();     // document是一条记录，包含多个field
                document.add(new LongPoint("questionId",questionId));    // int,long,float 这些number是不支持分词的,默认就不支持存储
                document.add(new StoredField("questionId",questionId));  // Long类型Field默认不支持存储，需用storeField
                document.add(new TextField("question",title, Field.Store.YES)); // textfield：会分词，lucene倒排索引
                writer.addDocument(document);
                System.out.println("本次共索引"+writer.numDocs()+"条");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            directory.close();
        }
        return true;
    }

    @Override
    public boolean createAnswerIndex() {
        int limit = Math.toIntExact(answerService.selectAnswerCount());
        List<Answer> answers = answerService.queryAnswers(0,limit);
        FSDirectory directory = null;
        try {
            directory = FSDirectory.open(Paths.get(IDX_ANSWER));
        } catch (IOException e) {
            e.printStackTrace();
        }
        IKAnalyzer analyzer = new IKAnalyzer(true);
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);    //每次索引删除之前的
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(directory,writerConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Answer answer : answers){
            Long answerId = answer.getAnswerId();
            //System.out.println(answerId);
            String content = answer.getContent();
            //System.out.println(content);
            Document document = new Document();
            document.add(new LongPoint("answerId",answerId));
            document.add(new StoredField("answerId",answerId));  //默认不支持存储
            document.add(new TextField("answer",content, Field.Store.YES));
            try {
                writer.addDocument(document);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("本次共索引"+writer.numDocs()+"条");
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            directory.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public List<IndexToken> searchAnswerIds(String content, int page, int limit){
        IKAnalyzer ikAnalyzer = new IKAnalyzer(true);   // 初始化IK分词器
        FSDirectory directory = null;
        try {
            directory = FSDirectory.open(Paths.get(IDX_ANSWER));    // 索引文件的路径
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexReader indexReader = null;
        try {
            indexReader = DirectoryReader.open(directory);      // 读取索引
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexSearcher searcher = new IndexSearcher(indexReader);    // 在单个IndexReader上实现搜索
        String filedName = "answer";    // 关键字所在的搜索属性
        QueryParser queryParser = new QueryParser(filedName, ikAnalyzer);   // 查询语义分析器
        Query query = null;
        try {
            query = queryParser.parse(content);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        TopScoreDocCollector results = TopScoreDocCollector.create(page*limit + limit, null);   // 限定返回条数
        TopDocs docs = null;
        try {
            searcher.search(query, results);    //n：返回查询参数result
        } catch (IOException e) {
            e.printStackTrace();
        }
        docs = results.topDocs(page*limit, limit);
        System.out.println("总共命中查询：" + docs.totalHits + "条");     // 查询命中总数
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(
                "<span style='color:red'>", "</span>");     // 用pre和post标记突出显示术语。
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));
        List<IndexToken> HeightString_ID = new ArrayList<>();   //返回的List
        for (ScoreDoc sdoc :docs.scoreDocs){
            int docId = sdoc.doc;   // lucene索引ID
            Document hitDoc = null;
            try {
                hitDoc = searcher.doc(sdoc.doc);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Long answerId = Long.valueOf(hitDoc.get("answerId"));   //获取该doc的answerId属性值
            TokenStream tokenStream = ikAnalyzer.tokenStream(filedName, new StringReader(hitDoc.get(filedName)));
            String bestFragment = null;
            try {
                bestFragment = highlighter.getBestFragment(tokenStream, hitDoc.get(filedName));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTokenOffsetsException e) {
                e.printStackTrace();
            }
//            System.out.println("LuceneId:" + docId + "\tanswerId:" + answerId + "\tanswer:" + bestFragment);
            IndexToken indexToken = new IndexToken();
            indexToken.setHighlight(bestFragment);
            indexToken.setId(answerId);
            HeightString_ID.add(indexToken);
        }
        try {
            indexReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            directory.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("第"+ page +"页的" + HeightString_ID.size() + "条：");
        int i;
        for (IndexToken testIT:HeightString_ID){
            System.out.println(testIT);
        }
        return HeightString_ID;
    }

    @Override
    public List<IndexToken> searchQuestionIds(String title, int page, int limit) {
        IKAnalyzer ikAnalyzer = new IKAnalyzer(true);   // 初始化IK分词器
        FSDirectory directory = null;
        try {
            directory = FSDirectory.open(Paths.get(IDX_QUESTION));    // 索引文件的路径
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexReader indexReader = null;
        try {
            indexReader = DirectoryReader.open(directory);      // 读取索引
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexSearcher searcher = new IndexSearcher(indexReader);    // 在单个IndexReader上实现搜索
        String filedName = "question";    // 关键字所在的搜索属性
        QueryParser queryParser = new QueryParser(filedName, ikAnalyzer);   // 查询语义分析器
        Query query = null;
        try {
            query = queryParser.parse(title);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        TopScoreDocCollector results = TopScoreDocCollector.create(page*limit + limit, null);   // 限定返回条数
        TopDocs docs = null;
        try {
            searcher.search(query, results);    //n：返回查询参数result
        } catch (IOException e) {
            e.printStackTrace();
        }
        docs = results.topDocs(page*limit, limit);
        System.out.println("总共命中查询：" + docs.totalHits + "条");     // 查询命中总数
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(
                "<span style='color:red'>", "</span>");     // 用pre和post标记突出显示术语。
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));
        List<IndexToken> HeightString_ID = new ArrayList<>();   //返回的List
        for (ScoreDoc sdoc :docs.scoreDocs){
            int docId = sdoc.doc;   // lucene索引ID
            Document hitDoc = null;
            try {
                hitDoc = searcher.doc(sdoc.doc);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Long questionId = Long.valueOf(hitDoc.get("questionId"));   //获取该doc的questionId属性值
            TokenStream tokenStream = ikAnalyzer.tokenStream(filedName, new StringReader(hitDoc.get(filedName)));
            String bestFragment = null;
            try {
                bestFragment = highlighter.getBestFragment(tokenStream, hitDoc.get(filedName));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTokenOffsetsException e) {
                e.printStackTrace();
            }
//            System.out.println("LuceneId:" + docId + "\tquestionId:" + questionId + "\tquestion:" + bestFragment);
            IndexToken indexToken = new IndexToken();
            indexToken.setHighlight(bestFragment);
            indexToken.setId(questionId);
            HeightString_ID.add(indexToken);
        }
        try {
            indexReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            directory.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("第"+ page +"页的" + HeightString_ID.size() + "条：");
        for (IndexToken testIT:HeightString_ID){
            System.out.println(testIT);
        }
        return HeightString_ID;
    }
}
