import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;


public class EntryPoint {

	static Random rand=new Random();

	
	
	public static void main(String[] args) throws IOException {
			
		GenerateData generateDataSets=new GenerateData();
		generateDataSets.myListGenerator();
		generateDataSets.Friends();
		generateDataSets.AccessLog();
	}
	}
