package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;

/**
 * GestionMedicos: Implementa la gestion de medicos segun el PDF de la carpeta
 * enunciado
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @version 1.0
 * @since 1.0
 */
public class GestionMedicos {

	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException {
		tests();

		System.out.println("FIN.............");
	}

	public static void reservar_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta)
			throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement insert_linea = null;
		PreparedStatement update_linea = null;
		con = pool.getConnection();

		java.sql.Date m_sqlFecha_Consulta = new java.sql.Date(m_Fecha_Consulta.getTime());

		try {
			ResultSet resultSet;

			// Comprobamos si existen medicos
			PreparedStatement select_medico = con.prepareStatement("SELECT COUNT(*) FROM medico WHERE NIF = ?");
			select_medico.setString(1, m_NIF_medico);
			resultSet = select_medico.executeQuery();
			// System.out.println(resultSet.next());
			if (!resultSet.next()) {
				throw new GestionMedicosException(2);
			}
			resultSet.close();
			select_medico.close();

			// Sacar id_medico para eliminar registros de tabla consultas
			String sqlMedico = "SELECT id_medico FROM medico WHERE NIF = ?";
			PreparedStatement sacaId = con.prepareStatement(sqlMedico);
			sacaId.setString(1, m_NIF_medico);
			resultSet = sacaId.executeQuery();

			// Comprobamos si existe la consulta
			PreparedStatement select_consulta = con
					.prepareStatement("SELECT COUNT(*) FROM medico WHERE id_medico = ? and fecha_consulta = ?");
			select_consulta.setString(1, resultSet.getString(1));
			select_consulta.setDate(2, m_sqlFecha_Consulta);
			ResultSet resultSet1 = select_consulta.executeQuery();
			if (resultSet1.next()) {
				throw new GestionMedicosException(3);
			}

			resultSet.close();
			resultSet1.close();

			String sqlPaciente = "SELECT COUNT(*) FROM cliente WHERE NIF = ?";
			PreparedStatement compruebaPaciente = con.prepareStatement(sqlPaciente);
			resultSet = compruebaPaciente.executeQuery();

			if (!resultSet.next()) {
				throw new GestionMedicosException(1);
			}
			resultSet.close();

			insert_linea = con.prepareStatement("INSERT INTO consulta values (sec_id_consulta.nextVal,?,?,?)");
			insert_linea.setDate(1, m_sqlFecha_Consulta);
			insert_linea.setString(2, m_NIF_cliente);
			insert_linea.setString(3, m_NIF_medico);
			insert_linea.executeUpdate();

			update_linea = con.prepareStatement("UPDATE medico SET consultas = consultas + 1 WHERE NIF = ?");
			update_linea.setString(1, m_NIF_medico);
			update_linea.executeUpdate();

			con.commit();

		} catch (SQLException e) {
			if (con != null) {
				con.rollback();
			}

			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException) e;
			}

		} finally {
			if (insert_linea != null) {
				insert_linea.close();
			}
			if (update_linea != null) {
				update_linea.close();
			}
			if (con != null) {
				con.close();
			}
		}

	}

	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta,
			Date m_Fecha_Anulacion) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;

		PreparedStatement select_idmed = null;
		PreparedStatement select_linea = null;
		PreparedStatement insert_linea = null;
		PreparedStatement update_table = null;

		ResultSet res1 = null;
		ResultSet res2 = null;

		try {
			con = pool.getConnection();

			ResultSet resultSet;
			
			//Comprobamos si existe paciente
			
			String sqlPaciente = "SELECT COUNT(*) FROM cliente WHERE NIF = ?";
			PreparedStatement compruebaPaciente = con.prepareStatement(sqlPaciente);
			resultSet = compruebaPaciente.executeQuery();

			if (!resultSet.next()) {
				throw new GestionMedicosException(1);
			}
			resultSet.close();

			// Comprobamos si existen medicos
			PreparedStatement select_medico = con.prepareStatement("SELECT COUNT(*) FROM medico WHERE NIF = ?");
			select_medico.setString(1, m_NIF_medico);
			resultSet = select_medico.executeQuery();
			// System.out.println(resultSet.next());
			if (!resultSet.next()) {
				throw new GestionMedicosException(2);
			}

			select_idmed = con.prepareStatement("select id_medico from medico where NIF = ?");

			select_idmed.setString(1, m_NIF_medico);

			res1 = select_idmed.executeQuery();

			int id_medico = res1.getInt(1);
			// Comprobamos si existe la consulta

			select_linea = con.prepareStatement(
					"select id_consulta from consulta where fecha_consulta = ? and NIF = ? and id_medico = ?");
			select_linea.setDate(1, new java.sql.Date(m_Fecha_Consulta.getTime()));
			select_linea.setString(2, m_NIF_cliente);
			select_linea.setInt(3, id_medico);

			res2 = select_linea.executeQuery();

			int id_consulta = -1;
			if (res2.next()) {
				id_consulta = res2.getInt(1);
			} else {
				throw new GestionMedicosException(GestionMedicosException.CONSULTA_NO_EXISTE);

			}

			// Asegurarnos de que al menos se produce 2 días antes
			long difdias = m_Fecha_Consulta.getTime() - m_Fecha_Anulacion.getTime();
			long dias = TimeUnit.DAYS.convert(difdias, TimeUnit.MILLISECONDS);
			if (dias < 2) {
				throw new GestionMedicosException(GestionMedicosException.CONSULTA_NO_ANULA);
			}

			// Insertar en tabla de anulación

			insert_linea = con.prepareStatement(
					"INSERT INTO anulacion (id_anulacion,fecha_anulacion, id_consulta) VALUES (sec_id_anulacion.nextVal,?, ?)");
			insert_linea.setDate(1, (java.sql.Date) m_Fecha_Anulacion);
			insert_linea.setInt(2, id_consulta);

			insert_linea.executeQuery();

			// Cambiar valor consultas a 0

			update_table = con.prepareStatement("UPDATE medico SET consultas = consultas - 1 WHERE id_medico = ?");
			update_table.setInt(1, id_medico);
			update_table.executeUpdate();

			con.commit();

		} catch (SQLException e) {

			if (con != null) {
				con.rollback();
			}

			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException) e;
			}

		} finally {
			if (res1 != null || res2 != null) {
				res1.close();
				res2.close();
			}
			if (select_idmed != null) {
				select_idmed.close();
			}
			if (select_linea != null) {
				select_linea.close();
			}

			if (insert_linea != null) {
				insert_linea.close();
			}
			if (update_table != null) {
				update_table.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}

	public static void eliminar_medico(String m_NIF_medico) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement stmt = null;

		try {
			con = pool.getConnection();

			// Desactivar autocommit para iniciar una transacción
			con.setAutoCommit(false);

			ResultSet resultSet;

			// Comprobamos si existen medicos
			PreparedStatement select_medico = con.prepareStatement("SELECT COUNT(*) FROM medico WHERE NIF = ?");
			select_medico.setString(1, m_NIF_medico);
			resultSet = select_medico.executeQuery();
			// System.out.println(resultSet.next());
			if (!resultSet.next()) {
				throw new GestionMedicosException(2);
			}
			resultSet.close();
			select_medico.close();

			// Eliminar los registros de otras tablas
			eliminarRegistrosAsociados(con, m_NIF_medico);

			// Eliminar el médico de la tabla medico
			String sql = "DELETE FROM medico WHERE NIF= ?";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, m_NIF_medico);
			stmt.executeUpdate();

			// Confirmar la transacción
			con.commit();

		} catch (SQLException e) {
			// En caso de error, hacer rollback de la transacción
			if (con != null) {
				con.rollback();
			}

			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException) e;
			}

		} finally {

			if (stmt != null) {
				stmt.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}

	private static void eliminarRegistrosAsociados(Connection con, String m_NIF_medico) throws SQLException {
		int id_medico = 0;
		// Sacar id_medico para eliminar registros de tabla consultas
		String sqlMedico = "SELECT id_medico FROM medico WHERE NIF = ?";
		PreparedStatement sacaId = con.prepareStatement(sqlMedico);
		sacaId.setString(1, m_NIF_medico);
		ResultSet resultSet = sacaId.executeQuery();

		id_medico = resultSet.getInt(1);

		// Eliminar registros asociados en la tabla "citas"
		String sqlConsultas = "DELETE FROM consultas WHERE id_medico = ?";
		PreparedStatement stmtCitas = con.prepareStatement(sqlConsultas);
		stmtCitas.setInt(1, id_medico);
		stmtCitas.executeUpdate();

	}

	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
	}

	static void tests() throws SQLException {

		creaTablas();

		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		SimpleDateFormat date = new SimpleDateFormat("dd/mm/yyyy");
		Date parsed = null;
		Date anulacion = null;

		// Relatar caso por caso utilizando el siguiente procedure para inicializar los
		// datos

		CallableStatement cll_reinicia = null;
		Connection conn = null;

		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			try {

				try {
					parsed = date.parse("25/03/2022");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date fecha = new java.sql.Date(parsed.getTime());

				reservar_consulta("12345678A", "123A", fecha);
				System.out.println("NO se da cuenta de que el medico esta MAL");
			} catch (GestionMedicosException e) {
				if (e.getErrorCode() == GestionMedicosException.MEDICO_NO_EXISTE) {
					System.out.println(e.getMessage());
					System.out.println("Detecta medico no existe");
				}

			}

			try {
				try {
					parsed = date.parse("25/03/2022");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date fecha = new java.sql.Date(parsed.getTime());
				reservar_consulta("12345678A", "8766788Y", fecha);
				System.out.println("NO detecta que el medico esta ocupado");
			} catch (GestionMedicosException e) {
				if (e.getErrorCode() == GestionMedicosException.MEDICO_OCUPADO) {
					System.out.println(e.getMessage());
					System.out.println("Detecta ocupado");
				}

			}

			try {
				try {
					parsed = date.parse("25/03/2022");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date fecha = new java.sql.Date(parsed.getTime());
				reservar_consulta("1278A", "8766788Y", fecha);
				System.out.println("NO detecta que el paciente NO existe");
			} catch (GestionMedicosException e) {
				if (e.getErrorCode() == GestionMedicosException.MEDICO_OCUPADO) {
					System.out.println(e.getMessage());
					System.out.println("Detecta paciente MAL");
				}

			}
			
			try {
				try {
					parsed = date.parse("20/03/2022");
					anulacion=date.parse("24/02/2023");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date fecha = new java.sql.Date(parsed.getTime());
				anular_consulta("12345678Y", "8766788Y", fecha, anulacion);
				System.out.println("NO detecta que la consulta NO existe");
			} catch (GestionMedicosException e) {
				if (e.getErrorCode() == GestionMedicosException.CONSULTA_NO_EXISTE) {
					System.out.println(e.getMessage());
					System.out.println("Detecta consulta NO existe");
				}

			}
			
			try {
				try {
					parsed = date.parse("24/03/2022");
					anulacion=date.parse("23/03/2023");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date fecha = new java.sql.Date(parsed.getTime());
				anular_consulta("12345678Y", "8766788Y", fecha, anulacion);
				System.out.println("NO detecta que la consulta NO se puede anular");
			} catch (GestionMedicosException e) {
				if (e.getErrorCode() == GestionMedicosException.CONSULTA_NO_ANULA) {
					System.out.println(e.getMessage());
					System.out.println("Detecta consulta NO se puede anular");
				}

			}

			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();

		}

	}

}
