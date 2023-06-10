package lsi.ubu.solucion;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import lsi.ubu.util.*;

public class GestionMedicos {
	public static void reservar_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta)
			throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;

		PreparedStatement insert_linea = null;
		PreparedStatement update_linea = null;

		java.sql.Date m_sqlFecha_Consulta = new java.sql.Date(m_Fecha_Consulta.getTime());

		try {
			con = pool.getConnection();

			// Se añade la nueva consulta
			insert_linea = con.prepareStatement(
					"INSERT INTO consultas values (sec_id_consulta.nextVal,?,?,select id_medico from medicos where NIF = ?)");
			insert_linea.setDate(1, m_sqlFecha_Consulta);
			insert_linea.setString(2, m_NIF_cliente);
			insert_linea.setString(3, m_NIF_medico);
			insert_linea.executeUpdate();

			// Se actualiza el campo consulta en la tabla medicos

			update_linea = con.prepareStatement("UPDATE medico SET consultas = 1 WHERE NIF = ?");
			update_linea.setString(1, m_NIF_medico);

			update_linea.executeUpdate();

			con.commit();
		} catch (SQLException e) {
			if (con != null) {

				con.rollback();
			}
			throw e;
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
			Date m_Fecha_Anulacion, String motivo) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;

		PreparedStatement select_idmed = null;
		PreparedStatement select_linea = null;
		PreparedStatement insert_linea = null;
		PreparedStatement update_table = null;
		
		ResultSet res = null;

		try {
			con = pool.getConnection();
			
			//Identificamos el id que se corresponde con el NIF del medico
			
			select_idmed = con.prepareStatement("select id_medico from medico where NIF = ?");
			
			select_idmed.setString(1, m_NIF_medico);
			
			res = select_idmed.executeQuery();
			
			int id_medico = res.getInt("id_medico");
			

			// Comprobamos si existe la consulta

			select_linea = con.prepareStatement(
					"select id_consulta from consulta where fecha_consulta = ? and NIF = ? and id_medico = ?");
			select_linea.setDate(1, new java.sql.Date(m_Fecha_Consulta.getTime()));
	        select_linea.setString(2, m_NIF_cliente);
	        select_linea.setInt(3, id_medico);
	        
	        res = select_linea.executeQuery();
	        
	        int id_consulta = -1;
	        if (res.next()) {
	        	id_consulta=res.getInt("id_consulta");
	        } else {
	        	throw new SQLException("No se encontro consulta");
	  
	        }
	        
	        //Asegurarnos de que al menos se produce 2 días antes
	        long difdias = m_Fecha_Consulta.getTime() - m_Fecha_Anulacion.getTime();
	        long dias = TimeUnit.DAYS.convert(difdias,TimeUnit.MILLISECONDS);
	        if (dias < 2) {
	        	throw new SQLException("Quedan menos de 2 días para la consulta");
	        }
	        
	      //Insertar en tabla de anulación
        	
        	insert_linea = con.prepareStatement("INSERT INTO anulacion (motivo_anulacion, fecha_anulacion, id_consulta) VALUES (?, ?, ?)");
        	insert_linea.setString(1, motivo);
            insert_linea.setDate(2, m_Fecha_Anulacion);
            insert_linea.setInt(3, id_consulta);
            
            insert_linea.executeQuery();
            
            //Cambiar valor consultas a 0
            
            update_table = con.prepareStatement("UPDATE medico SET consultas = consultas - 1 WHERE id_medico = ?");
            update_table.setInt(1, id_medico);
            update_table.executeUpdate();
            
            con.commit();
		} catch (SQLException e) {
			if (con != null) {
				con.rollback();
			}
			throw e;
		} finally {
			if (res != null) {
	            res.close();
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

	public static void consulta_medico(String m_NIF_medico) throws SQLException {

	}

}
