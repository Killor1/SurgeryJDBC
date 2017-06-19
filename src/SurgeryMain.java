import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;

public class SurgeryMain {

	public static void main(String[] args) throws IOException, SQLException, ParseException {
		Menu menu = new Menu();
		Connection conn = null;
		
		int option;
		int intents = 0;
		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();
		
		conn = dbaccessor.getConnection();
		option = menu.menuPral();
		while (option > 0 && option < 6) {
			switch (option) {
			case 1:
				dbaccessor.mostraDoctorsPerEspecialitat();
				break;

			case 2:
				dbaccessor.altaVisita();
				break;

			case 3:
				dbaccessor.modificaAdrecaPacient();
				break;

			case 4:
				dbaccessor.modificaSalariDoctor();
				break;

			case 5:
				dbaccessor.carregaVisites();
				break;

			case 6:
				dbaccessor.sortir();
					

			default:
				System.out.println("Introdueixi una de les opcions anteriors");
				break;

			}
			option = menu.menuPral();
		}

	}

}
