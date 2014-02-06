import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class TestForFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try{
			
		
		File file= new File("empsal.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String s = null;
		while((s=br.readLine())!=null)
		{
			String myarr[]=s.split(",");
			for(int i=0;i<5;i++)
			{
				System.out.println(myarr[i]);
			}
		}
		}
		catch(Exception e)
		{
			
		}
	}

}
