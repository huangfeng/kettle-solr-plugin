/**********************************************************************
 **                                                                   **
 **               This code belongs to the KETTLE project.            **
 **                                                                   **
 ** Kettle, from version 2.2 on, is released into the public domain   **
 ** under the Lesser GNU Public License (LGPL).                       **
 **                                                                   **
 ** For more details, please read the document LICENSE.txt, included  **
 ** in this project                                                   **
 **                                                                   **
 ** http://www.kettle.be                                              **
 ** info@kettle.be                                                    **
 **                                                                   **
 **********************************************************************/

package be.ibridge.kettle.trans.step.solrin;

import be.ibridge.kettle.core.Const;
import be.ibridge.kettle.core.Row;
import be.ibridge.kettle.core.exception.KettleException;
import be.ibridge.kettle.core.exception.KettleStepException;
import be.ibridge.kettle.core.value.Value;
import be.ibridge.kettle.trans.Trans;
import be.ibridge.kettle.trans.TransMeta;
import be.ibridge.kettle.trans.step.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.net.MalformedURLException;

/**
 * Retrieves values from a database by calling database stored procedures or functions
 *
 * @author Ian Holsman (based on Matt's HTTP)
 * @since 8-July-2007
 */

public class SolrIn extends BaseStep implements StepInterface {
    private SolrInMeta meta;
    private SolrInData data;
    protected String url;
    protected SolrServer server;
    protected int rowCount;

    public static final String POST_ENCODING = "UTF-8";

    public SolrIn(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    private void execHttp(Row row) throws KettleException {
        if (first) {
            first = false;
            data.argnrs = new int[meta.getArgumentField().length];

            for (int i = 0; i < meta.getArgumentField().length; i++) {
                data.argnrs[i] = row.searchValueIndex(meta.getArgumentField()[i]);
                if (data.argnrs[i] < 0) {
                    logError(Messages.getString("SolrIn.Log.ErrorFindingField") + meta.getArgumentField()[i] + "]"); //$NON-NLS-1$ //$NON-NLS-2$
                    throw new KettleStepException(Messages.getString("SolrIn.Exception.CouldnotFindField", meta.getArgumentField()[i])); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        }

        Value result = callHttpService(row);
        row.addValue(result);
    }

    private Value callHttpService(Row row) throws KettleException {


        UpdateResponse upres;

        try {

            SolrInputDocument doc = new SolrInputDocument();
            logDetailed("Connecting to : [" + url + "]");

            for (int i = 0; i < data.argnrs.length; i++) {
                doc.addField(//meta.getArgumentField()[i],
                        meta.getArgumentParameter()[i],
                        row.getValue(data.argnrs[i]).toString(false));

            }

            upres = server.add(doc);
           // logDetailed("ADD:" + upres.getResponse());
            if (upres == null) {
                 throw new KettleException("Unable add document URL :" + url + ": null response" );  
            }
            if (upres == null && ( upres.getStatus() != 0)) {
                throw new KettleException("Unable add document URL :" + url + ":" + upres.getResponse());
            }
            rowCount++;
            if ( rowCount == 1000 ) {
                logDetailed("Commit ever 1000 records (hard coded)");
                server.commit();
                rowCount  =0;
            }

        }

        catch (Exception e)

        {
            throw new KettleException("Unable to get result from specified URL :" + url, e);
        }


        return new Value("0");
    }


    public boolean processRow
            (StepMetaInterface
                    smi, StepDataInterface
                    sdi) throws KettleException {
        meta = (SolrInMeta) smi;
        data = (SolrInData) sdi;

        Row r = getRow();       // Get row from input rowset & set row busy!
        if (r == null)  // no more input to be expected...
        {
            setOutputDone();
            return false;
        }

        try {
            execHttp(r); // add new values to the row
            putRow(r);  // copy row to output rowset(s);

            if (checkFeedback(linesRead))
                logBasic(Messages.getString("SolrIn.LineNumber") + linesRead); //$NON-NLS-1$
        }
        catch (KettleException e) {
            logError(Messages.getString("SolrIn.ErrorInStepRunning") + e.getMessage()); //$NON-NLS-1$
            setErrors(1);
            stopAll();
            setOutputDone();  // signal end to receiver(s)
            return false;
        }

        return true;
    }

    public boolean init
            (StepMetaInterface
                    smi, StepDataInterface
                    sdi) {
        meta = (SolrInMeta) smi;
        data = (SolrInData) sdi;
           url = meta.getUrl();
        try {
            server = new CommonsHttpSolrServer(url);
            ((CommonsHttpSolrServer) server).setConnectionTimeout(5);
            ((CommonsHttpSolrServer) server).setDefaultMaxConnectionsPerHost(100);
            ((CommonsHttpSolrServer) server).setMaxTotalConnections(100);
        } catch ( MalformedURLException e) {
            logError( "URL:" + url +" is invalid.");
            return false;
        }
        return super.init(smi, sdi);
    }

    public void dispose
            (StepMetaInterface
                    smi, StepDataInterface
                    sdi) {
        meta = (SolrInMeta) smi;
        data = (SolrInData) sdi;

        super.dispose(smi, sdi);
    }

    //
    // Run is were the action happens!
    public void run
            () {
        logBasic(Messages.getString("SolrIn.Log.StartingToRun")); //$NON-NLS-1$

        try {
            while (processRow(meta, data) && !isStopped()) ;
        }
        catch (Exception e) {
            logError(Messages.getString("SolrIn.Log.UnexpectedError") + " : " + e.toString()); //$NON-NLS-1$ //$NON-NLS-2$
            logError(Const.getStackTracker(e));
            setErrors(1);
            stopAll();
        }
        finally {
            dispose(meta, data);
            logSummary();
            markStop();
        }
    }

    public String toString
            () {
        return this.getClass().getName();
    }
}
