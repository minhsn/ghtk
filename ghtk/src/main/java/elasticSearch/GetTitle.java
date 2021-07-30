package elasticSearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;


import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GetTitle {
    public static List<String> VIETNAMESE_DIACRITIC_CHARACTERS = Arrays.asList("[A|Ắ|Ằ|Ẳ|Ẵ|Ặ|Ă|Ấ|Ầ|Ẩ|Ẫ|Ậ|Â|Á|À|Ã|Ả|Ạ]", "[D|Đ]","[E|Ế|Ề|Ể|Ễ|Ệ|Ê|É|È|Ẻ|Ẽ|Ẹ]","[I|Í|Ì|Ỉ|Ĩ|Ị]","[O|Ố|Ồ|Ổ|Ỗ|Ộ|Ô|Ớ|Ờ|Ở|Ỡ|Ợ|Ơ|Ó|Ò|Õ|Ỏ|Ọ]","[U|Ứ|Ừ|Ử|Ữ|Ự|Ư|Ú|Ù|Ủ|Ũ|Ụ]","[Y|Ý|Ỳ|Ỷ|Ỹ|Ỵ]");
    public static void main(String[] args) throws IOException {
        Scanner myObj = new Scanner(System.in);
        System.out.println("Type your word");
        String word = myObj.nextLine();

        List<String> words=genVnWords(word);
        List<String> result=new ArrayList<String>();

        for (String i :words){
            result.addAll(0,getSuggestion(i));
        }
;
        System.out.print(removeDuplicates(result));

    }


    public static List<String> getSuggestion(String input) throws IOException { //lấy từ gợi ý từ elastic search
        RestHighLevelClient client=new RestHighLevelClient(RestClient.builder(new HttpHost("10.140.0.10",9200,"http")));


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.completionSuggestion("suggest_title").regex(input).skipDuplicates( true ).size(50);

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("title-suggest", termSuggestionBuilder);

        searchSourceBuilder.suggest(suggestBuilder);

        SearchRequest searchRequest = new SearchRequest("dantri");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(searchResponse.getSuggest().iterator(), Spliterator.ORDERED), false)
                .flatMap(suggestion -> suggestion.getEntries().get(0).getOptions().stream())
                .map((Suggest.Suggestion.Entry.Option option) -> option.getText().toString())
                .filter(c->c.indexOf(" ")>=0)
                .map(c->c.toLowerCase())
                .collect(Collectors.toList());
    }



    public static List<String> removeDuplicates(List<String> list) // xóa các phần tử lặp trong list
    {

        List<String> newList = new ArrayList<>();

        for (String element : list) {

            if (!newList.contains(element)) {

                newList.add(element);
            }
        }

        return newList;
    }
    public static List<String> genVnWords(String input){ //dùng để biến đổi từ input thành dạng regex
        String replaceString="";
        List<String> newList = new ArrayList<>();
        newList.add(input);
        for (char i : input.toCharArray()){
            if (i =='a' | i=='A' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(0));
                newList.add(replaceString);
            }
            if (i =='e' | i=='E' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(2));
                newList.add(replaceString);
            }
            if (i =='i' | i=='I' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(3));
                newList.add(replaceString);               }
            if (i =='o' | i=='O' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(4));
                newList.add(replaceString);           }
            if (i =='u' | i=='U' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(5));
                newList.add(replaceString);
                }
            if (i =='y' | i=='Y' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(6));
                newList.add(replaceString);
                }
            if (i =='d' | i=='D' ){
                replaceString = input.replace(Character.toString(i),VIETNAMESE_DIACRITIC_CHARACTERS.get(1));
                newList.add(replaceString);
            }
        }
        return newList;
    }
}
