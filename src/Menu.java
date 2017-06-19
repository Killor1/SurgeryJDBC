import java.io.*;

public class Menu {
	private int option;

	public Menu() {
		super();
	}

	public int menuPral() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		do {
			System.out.println(" \nSURGERY \n");
			System.out.println(" \n======= \n");
			System.out.println(" \nMENU PRINCIPAL \n");
			System.out.println("1. Mostra Doctors per Especialitat ");
			System.out.println("2. Alta visita. ");
			System.out.println("3. Modifica adreça+ciutat d'un Pacient ");
			System.out.println("4. Modifica Salari Doctor ");
			System.out.println("5. Carrega visites ");
			System.out.println("6. Sortir.  ");
			

			System.out.println("Esculli opció: ");
			try {
				option = Integer.parseInt(br.readLine());
			} catch (NumberFormatException | IOException e) {
				System.out.println("valor no vàlid");
				e.printStackTrace();

			}

		} while (option != 1 && option != 2 && option != 3 && option != 4 && option != 5 );

		return option;
	}

	

}
