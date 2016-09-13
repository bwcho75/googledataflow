package com.terry.nl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPI;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPI.Documents.AnnotateText;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPIScopes;
import com.google.api.services.language.v1beta1.model.AnalyzeEntitiesRequest;
import com.google.api.services.language.v1beta1.model.AnalyzeEntitiesResponse;
import com.google.api.services.language.v1beta1.model.AnalyzeSentimentRequest;
import com.google.api.services.language.v1beta1.model.AnalyzeSentimentResponse;
import com.google.api.services.language.v1beta1.model.AnnotateTextRequest;
import com.google.api.services.language.v1beta1.model.AnnotateTextResponse;
import com.google.api.services.language.v1beta1.model.Document;
import com.google.api.services.language.v1beta1.model.Entity;
import com.google.api.services.language.v1beta1.model.Features;
import com.google.api.services.language.v1beta1.model.Sentiment;
import com.google.api.services.language.v1beta1.model.Token;

import java.io.IOException;
import java.io.PrintStream;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

/**
 * 
 * Google Cloud NL API wrapper
 */


@SuppressWarnings("serial")
public class NLAnalyze {

	public static NLAnalyze getInstance() throws IOException,GeneralSecurityException {

		return new NLAnalyze(getLanguageService());
	}

	public NLAnalyzeVO analyze(String text) throws IOException, GeneralSecurityException{
		Sentiment  s = analyzeSentiment(text);
		List <Token> tokens = analyzeSyntax(text);
		NLAnalyzeVO vo = new NLAnalyzeVO();

		for(Token token:tokens){
			String tag = token.getPartOfSpeech().getTag();
			String word = token.getText().getContent();

			if(tag.equals("NOUN")) vo.addNouns(word);
			else if(tag.equals("ADJ")) vo.addAdj(word);
		}

		vo.setSentimental(s.getPolarity());

		return vo;
	}


	/**
	 * Be sure to specify the name of your application. If the application name is {@code null} or
	 * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
	 */
	private static final String APPLICATION_NAME = "Google-LanguagAPISample/1.0";

	/**
	 * Connects to the Natural Language API using Application Default Credentials.
	 */
	public static CloudNaturalLanguageAPI getLanguageService() 
			throws IOException, GeneralSecurityException {
		GoogleCredential credential =
				GoogleCredential.getApplicationDefault().createScoped(CloudNaturalLanguageAPIScopes.all());
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
		return new CloudNaturalLanguageAPI.Builder(
				GoogleNetHttpTransport.newTrustedTransport(),
				jsonFactory, new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest request) throws IOException {
						credential.initialize(request);
					}
				})
				.setApplicationName(APPLICATION_NAME)
				.build();
	}

	private final CloudNaturalLanguageAPI languageApi;

	/**
	 * Constructs a {@link Analyze} which connects to the Cloud Natural Language API.
	 */
	public NLAnalyze(CloudNaturalLanguageAPI languageApi) {
		this.languageApi = languageApi;
	}

	public List<Token> analyzeSyntax(String text) throws IOException{
		AnnotateTextRequest request =
				new AnnotateTextRequest()
				.setDocument(new Document().setContent(text).setType("PLAIN_TEXT"))
				.setFeatures(new Features().setExtractSyntax(true))
				.setEncodingType("UTF16");
		AnnotateText analyze =
				languageApi.documents().annotateText(request);

		AnnotateTextResponse response = analyze.execute();

		return response.getTokens();

	}
	/**
	 * Gets {@link Sentiment} from the string {@code text}.
	 */
	public Sentiment analyzeSentiment(String text) throws IOException {
		AnalyzeSentimentRequest request =
				new AnalyzeSentimentRequest()
				.setDocument(new Document().setContent(text).setType("PLAIN_TEXT"));
		CloudNaturalLanguageAPI.Documents.AnalyzeSentiment analyze =
				languageApi.documents().analyzeSentiment(request);

		AnalyzeSentimentResponse response = analyze.execute();
		return response.getDocumentSentiment();
	}

}