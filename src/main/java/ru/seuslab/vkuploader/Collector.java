package ru.seuslab.vkuploader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;


public class Collector {

	
	public static void main(String[] args) {
		Collector collector = new Collector();
		try {
			collector.readOptions(); // чтение настроек
			collector.dbConnect();   // подключение к БД
			if(collector.collect())  // сбор данных о пользователях
				collector.uploadToXlsx(); // выгрузка данных в файл xlsx
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

 	
	public static ArrayList<Long> idsQueue = new ArrayList<>(); // очередь id для чтения из ВК 
	
	static ArrayList<VK_reader> vkReaderThreads = new ArrayList<>(); // список потоков чтения
	
	private String dbConnStr;  // строка подлючения к БД, без имени и пароля 
	private String dbUser; // имя пользователя БД
	private String dbPwd;  // пароль пользователя БД
	private Connection dbConn;  
	private Statement dbCmd; 
	private String vkAccessToken; // access_token для доступа к API ВК
	private int threadCount;  // количество потоков для чтения
	private int reqUsersCount;  // кол-во пользователей в запросе к API ВК
	private int readDelayTime;  // задержка (мс) при чтении API, иначе ВК ругается (Too many requests per second) 
	private String xlsxFileName;  // имя файла Excel

	
	public Collector() {
	}
	
	public void readOptions() throws Exception {
		System.out.println("Чтение настроек");
		InputStream is = ru.seuslab.vkuploader.Collector.class.getResourceAsStream("/config.properties");
		Properties props = new Properties();
		props.load(is);

		dbConnStr = props.getProperty("DB_CONNECTION_STRING"); // строка подлючения к БД, без имени и пароля
		dbUser = props.getProperty("DB_USER");    // имя пользователя БД
		dbPwd = props.getProperty("DB_PASSWORD"); // пароль пользователя БД
		vkAccessToken = props.getProperty("VK_ACCESS_TOKEN"); // access_token для доступа к API ВК
		threadCount=Integer.valueOf(props.getProperty("THREAD_COUNT", "1")); // количество потоков для чтения
		reqUsersCount=Integer.valueOf(props.getProperty("REQ_USERS_COUNT", "10"));  // кол-во пользователей в запросе к API ВК
		readDelayTime=Integer.valueOf(props.getProperty("READ_DELAY_TIME", "500")); // задержка (в мсек), иначе ВК ругается (Too many requests per second)
		xlsxFileName=props.getProperty("XLSX_FILE_NAME");  // имя выгружаемого файла Excel
		
		System.out.println("Строка подк. к БД: "+dbConnStr);
		System.out.println("Имя пользователя БД: "+dbUser);
		System.out.println("ВК access_token: "+vkAccessToken);
		System.out.println("Кол-во потоков для чтения: "+threadCount);
		System.out.println("Кол-во id в одном запросе: "+reqUsersCount);
		System.out.println("Задержка между запросами: "+readDelayTime); // (в мсек) иначе ВК ругается (Too many requests per second)
		System.out.println("Имя xlsx файла: "+xlsxFileName);
		if(vkAccessToken==null)
			throw new Exception("Ошибка, не задан VK_ACCESS_TOKEN");
		if(vkAccessToken.length()<60)
			throw new Exception("Ошибка, VK_ACCESS_TOKEN слишком короткий");
	}

	public void dbConnect() throws SQLException, ClassNotFoundException {
		System.out.println("Подключение к БД");
		// Class.forName("org.postgresql.Driver");
		dbConn = DriverManager.getConnection(dbConnStr, dbUser, dbPwd);
		dbCmd = dbConn.createStatement();
	}

	public boolean collect() throws SQLException {
		System.out.println("Заполнение списка ID для загрузки");
		String sql = 
			"SELECT user_id FROM user_info "+
			"WHERE user_processed IS NULL "+ // ещё не обработанные ID
			"ORDER BY user_id"; // LIMIT 900";
		ResultSet reader1 = dbCmd.executeQuery(sql);
		if (reader1.next()) {
			do {
				idsQueue.add(reader1.getLong("user_id"));
			} while (reader1.next());
		} else {
			System.out.println("Не найдено ID для загрузки, все пользователи обработаны");
			reader1.close();
			return true;
		}
		reader1.close();
		System.out.println("Найдено записей: "+idsQueue.size());
		
		System.out.println("Создание потоков для чтения API ВК и записи в БД");
		for(int t=0; t<threadCount; t++) {
			VK_reader vkR = new VK_reader();
			vkReaderThreads.add(vkR);
			vkR.setTrdNumber(t+1);
			vkR.setDbConnStr(dbConnStr);
			vkR.setDbUser(dbUser);
			vkR.setDbPwd(dbPwd);
			vkR.setVkAccessToken(vkAccessToken);
			vkR.setReqUsersCount(reqUsersCount);
			vkR.setReadDelayTime(readDelayTime);
			vkR.start();
		}
		
		// Цикл ожидания завершения потоков чтения
		while(true) {
			try {
				Thread.sleep(300); // ожидание 300мс
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			boolean allStopped=true;
			for(int t=0; t<threadCount; t++) {
				if(vkReaderThreads.get(t).getState()!=Thread.State.TERMINATED) {
					allStopped=false; // какой-то поток ещё работает
					break;
				}
			}
			if(allStopped) { // все потоки остановились
				System.out.println("Все потоки остановились");
				break; // выход из цикла ожидания
			}
		}

		if(idsQueue.size()>0) { // в очереди кто-то остался
			System.out.println("Остались незагруженные пользователи, запустите процесс ещё раз");
			return false;
		}
		System.out.println("Все пользователи загружены");
		return true;
	}
	
	
	public void uploadToXlsx() throws FileNotFoundException {
		System.out.println("Выгрузка в файл xlsx");
		try (FileOutputStream os = new FileOutputStream(xlsxFileName)) {
			Workbook wb = new Workbook(os, "VK_Users", "1.0");
			Worksheet ws = wb.newWorksheet("Sheet 1");

			StopWatch watch = new StopWatch();
			watch.start();

			ws.value(0, 0, "№ п/п");
			ws.value(0, 1, "ВК id");
			ws.value(0, 2, "имя");
			ws.value(0, 3, "фамилия");
			ws.value(0, 4, "дата рождения");
			ws.value(0, 5, "город");
			ws.value(0, 6, "контакты");
			ws.value(0, 7, "полит. взгляды");
			ws.value(0, 8, "религия");
			ws.value(0, 9, "наличие моб. тел.");

			String sql = 
				"SELECT user_id, user_f_name, user_l_name, user_b_date, user_city, user_contacts, user_political, user_religion, user_has_mobile "+
				"FROM user_info "+
				"WHERE user_processed IS NOT NULL "+ // обработанные ID
				"ORDER BY user_id"; // LIMIT 100";
			ResultSet reader1 = dbCmd.executeQuery(sql);
			int rowNum=1;
			while(reader1.next()) {
				ws.value(rowNum, 0, rowNum);
				ws.value(rowNum, 1, reader1.getString("user_id"));
				ws.value(rowNum, 2, reader1.getString("user_f_name"));
				ws.value(rowNum, 3, reader1.getString("user_l_name"));
				ws.value(rowNum, 4, reader1.getString("user_b_date"));
				ws.value(rowNum, 5, reader1.getString("user_city"));
				ws.value(rowNum, 6, reader1.getString("user_contacts"));
				ws.value(rowNum, 7, reader1.getString("user_political"));
				ws.value(rowNum, 8, reader1.getString("user_religion"));
				ws.value(rowNum, 9, reader1.getString("user_has_mobile"));
				rowNum++;
			}
			reader1.close();
			wb.finish();
			watch.stop();
			System.out.println("Выгружен файл "+xlsxFileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}		
}


// Класс чтения API ВК и записи информации о пользователях в БД
class VK_reader extends Thread {

	private int trdNumber;
	private String trdNum;
	private String dbConnStr;
	private String dbUser;
	private String dbPwd;
	private Connection dbConn;
	private Statement dbCmd;
	private ArrayList<Long> curIds = new ArrayList<>();
	private String vkAccessToken;
	private int reqUsersCount;
	private int readDelayTime; 
	private VK_Response usersArr;

	public void setTrdNumber(int val) {
		trdNumber=val;
	}
	
	public void setDbConnStr(String val) {
		dbConnStr=val;
	}
	
	public void setDbUser(String val) {
		dbUser=val;
	}

	public void setDbPwd(String val) {
		dbPwd=val;
	}

	public void setVkAccessToken(String val) {
		vkAccessToken=val;
	}
	
	public void setReqUsersCount(int val) {
		reqUsersCount=val;
	}
	
	public void setReadDelayTime(int val) {
		readDelayTime=val;
	}
	
	
	@Override
	public void run() {
		System.out.println("Запуск потока: " + trdNumber);
		try {
			trdNum = "Trd_"+trdNumber;
			System.out.println(trdNum+"  подключение к БД");
			dbConn = DriverManager.getConnection(dbConnStr, dbUser, dbPwd);
			dbCmd = dbConn.createStatement();
			// разбор очереди id
			while(true) {
				boolean isEmpty=false;
				curIds.clear();
				
				synchronized(Collector.idsQueue) {
					if(Collector.idsQueue.size()>0) {
						// выборка нескольких id из очереди
						for(int k=0; k<reqUsersCount; k++) { 
							curIds.add(Collector.idsQueue.get(0)); // первый в очереди
							Collector.idsQueue.remove(0); // удаление его из очереди
							if(Collector.idsQueue.size()==0) break; // очередь исчерпана
						}
					}
					else
						isEmpty=true; // очередь пуста
				}
				
				if(isEmpty)	break; // очередь пуста, завершение цикла (потока)
					
				try {
					getVkUsersInfo(); // чтение инфы о пользователях (curIds) через API ВК
					saveUsersInfo();  // запись полученной инфы в БД
				} catch (IOException e) {
					e.printStackTrace();
				}
				Thread.sleep(readDelayTime);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Завершение потока: " + trdNumber);
	}

	
	private void getVkUsersInfo() throws IOException {
		// curIds - порция id для чтения с ВК
		String ids = curIds.get(0).toString(); // первый есть всегда
		for(int k=1; k<curIds.size(); k++)
			ids += ","+curIds.get(k);
		System.out.println(trdNum+"  запрос пользователей: "+ids);
		
		String urlStr = "https://api.vk.com"; 
		urlStr += "/method/users.get?v=5.131";
		urlStr += "&access_token="+vkAccessToken;
		urlStr += "&user_ids="+ids;
		urlStr += "&fields=bdate,contacts,city,has_mobile,personal";
		urlStr += "&lang=ru";
		
		System.out.println(trdNum+"  запрос: " + urlStr);
		URL url = new URL(urlStr);
		URLConnection conn = url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
		conn.setRequestProperty("Accept", "application/json");
		conn.setDoOutput(true);
		
		InputStream is = conn.getInputStream();
		StringBuilder res = new StringBuilder();
		BufferedReader rd = new BufferedReader(new InputStreamReader(is));
		String line;
		while ((line = rd.readLine()) != null) {
			res.append(line);
		}
		rd.close();
		System.out.println(trdNum+"  "+res.toString());

		// Десериализация json-ответа
		ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
		String respJson=res.toString();
		usersArr = objectMapper.readValue(respJson, VK_Response.class);
	}
	
	
	private void saveUsersInfo() {
		System.out.println(trdNum+"  сохранение в БД");
		if(usersArr.response==null) {
			System.out.println(trdNum+"  нет данных для сохранения");
			return;
		}
		if(usersArr.response.length==0) {
			System.out.println(trdNum+"  нет данных для сохранения");
			return;
		}
		StringBuilder bigSql = new StringBuilder();
		int rowsCnt=0;
		for(int i=0; i<usersArr.response.length; i++) {
			if(usersArr.response[i].id>0) {
				bigSql.append("UPDATE user_info SET ");
				String user_f_name=usersArr.response[i].first_name;
				if(user_f_name.length()>100) user_f_name=user_f_name.substring(0,100);
				bigSql.append("user_f_name='"+user_f_name.replace("\'", "\'\'")+"',");
				
				String user_l_name=usersArr.response[i].last_name;
				if(user_l_name.length()>100) user_l_name=user_l_name.substring(0,100);
				bigSql.append("user_l_name='"+user_l_name.replace("\'", "\'\'")+"',");
				
				if(usersArr.response[i].bdate!=null) {
					String bDate = usersArr.response[i].bdate;
					int count = bDate.length() - bDate.replace(".", "").length();
					if(count==2) // полная дата (две точки)
						bigSql.append("user_b_date='"+bDate.replace("\'", "\'\'")+"',");
				}
				if(usersArr.response[i].city!=null)
					if(usersArr.response[i].city.title!=null) {
						String user_city=usersArr.response[i].city.title;
						if(user_city.length()>50) user_city=user_city.substring(0,50);
						bigSql.append("user_city='"+user_city.replace("\'", "\'\'")+"',");
					}
				if(usersArr.response[i].home_phone!=null) {
					String user_contacts=usersArr.response[i].home_phone;
					if(user_contacts.length()>100) user_contacts=user_contacts.substring(0,100);
					bigSql.append("user_contacts='"+user_contacts.replace("\'", "\'\'")+"',");
				}
				if(usersArr.response[i].personal!=null) {
					bigSql.append("user_political="+usersArr.response[i].personal.political+",");
					if(usersArr.response[i].personal.religion!=null) {
						String user_religion=usersArr.response[i].personal.religion;
						if(user_religion.length()>100) user_religion=user_religion.substring(0,100);
						bigSql.append("user_religion='"+user_religion.replace("\'", "\'\'")+"',");
					}
				}
				bigSql.append("user_has_mobile=");
				if(usersArr.response[i].has_mobile==1) bigSql.append("true,"); 
				else bigSql.append("false,");
				bigSql.append("user_processed=Now() ");
				bigSql.append("WHERE user_id="+usersArr.response[i].id+"; \r\n");
				rowsCnt++;
			}
		}
		try {
			dbCmd.execute(bigSql.toString());
			System.out.printf(trdNum+"  загружено записей: %d %n", rowsCnt);
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
