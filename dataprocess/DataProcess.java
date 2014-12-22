import java.io.*;

public class DataProcess {
	public static void main(String[] args) throws IOException {
		BufferedReader br = null;
		if (args.length != 2) {
			System.out.println("The input is:numrun\tdagType");
			System.exit(1);
		}

		for (int i = 100; i <= 100; i *= 2) {
			for (int j = 1; j <= 6; j++) {
				br = new BufferedReader(new FileReader("/home/kwang/Documents/work_kwang/javaprogram/SimMatrix/src/SimMatrix/Pipeline/summary." + i + "." + j + ".Pipeline"));
				String str = br.readLine();
				while (!str.startsWith("Througput")) {
					str = br.readLine();
				}
				String[] strLine = str.split(":");
				System.out.print(strLine[1] + "\t");
				br.close();
				br = null;
			}
			System.out.println();
		}
	}
}
