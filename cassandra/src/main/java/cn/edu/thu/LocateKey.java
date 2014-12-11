package cn.edu.thu;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * 输入tokens列表，列表是用./nodetool ring|awk '{print $1"\t"$8}' >tokens
 * 或者将将本程序main函数中的前半段替换掉，直接将tokens的初始化改成从token_sort文件中读取更佳（该文件由脚本gen-*那个脚本对应的源码文件SortTokens.java产生）
 * */
public class LocateKey {
	static TreeMap<Long,Integer> tokensMap=new TreeMap<Long,Integer>(new Comparator<Long>() {
		@Override
		public int compare(Long o1, Long o2) {
			return o1.compareTo(o2);
		}
	});
	static{
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader("tokens"));
			String line=null;
			while((line=reader.readLine())!=null){
				String[] tmp=line.split(",");
				tokensMap.put(Long.parseLong(tmp[0]),Integer.parseInt(tmp[1]));
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	private static int findIps(Long token){
		int b=-1;
		for(Map.Entry<Long, Integer> entry:tokensMap.entrySet()){
			if (token<entry.getKey()){
				b=entry.getValue(); 
				break;
			}
		}
		if(b==-1){
			return tokensMap.firstEntry().getValue();
		}else{
			return b;
		}
	}
	public static int getOnlyIp(String key){
//		System.out.println(findIps(getToken(key)));
		return findIps(getToken(key));
		
	}
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    /**
     * Generate the token of a key.
     * Note that we need to ensure all generated token are strictly bigger than MINIMUM.
     * In particular we don't want MINIMUM to correspond to any key because the range (MINIMUM, X] doesn't
     * include MINIMUM but we use such range to select all data whose token is smaller than X.
     */
    public static long getToken(String key1)
    {
    	ByteBuffer key=ByteBuffer.wrap(key1.getBytes(UTF_8));
        if (key.remaining() == 0)
            return (Long.MIN_VALUE);

        long hash = MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0)[0];
        return (normalize(hash));
    }
    private static long normalize(long v)
    {
        // We exclude the MINIMUM value; see getToken()
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
    }
	public static List<String> calculateNaturalEndpoints(String token, ArrayList<String> tokens,Map<String, String> tokenMap,int replicas)
    {
        List<String> endpoints = new ArrayList<String>(replicas);
        if (tokens.size()==0)
            return endpoints;
        //二分查找到第一个位置
        int loc=Collections.binarySearch(tokens, token,new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return new BigInteger(o1).compareTo(new BigInteger(o2));
			}
		});
        if(loc<0){
        	loc=-(loc+1);
        }
        while (endpoints.size() < replicas )
        {
            String ep = tokenMap.get(tokens.get(loc%tokens.size()));
            if (!endpoints.contains(ep))
                endpoints.add(ep);
            loc++;
        }
        return endpoints;
    }
	
	 public static void main(String[] args) throws IOException{
		 if(args.length<2){
				System.out.println("参数：1、文件名  2、 副本数");
			return;
		}
		File file=new File(args[0]);
		TreeMap<String, String> tokenMap=new TreeMap<String,String>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return new BigInteger(o1).compareTo(new BigInteger(o2));
			}
		});//token-->ip
		BufferedReader reader=new BufferedReader(new FileReader(file));
		String line=null;
		while((line=reader.readLine())!=null){
			String[] words=line.split("\t");
			if(words.length<2)
				continue;
			else if(line.length()<16)
				continue;
			tokenMap.put(words[1], words[0]);
		}
		ArrayList<String> tokens=new ArrayList<String>();
		reader.close();
		
		for(Entry<String, String> entry:tokenMap.entrySet()){
			tokens.add(entry.getKey());
		}
		
		String key="11Zabc";
		String token=getToken(key)+"";
		System.out.println(calculateNaturalEndpoints(token, tokens, tokenMap, Integer.valueOf(args[1])));
		
	 }
}
