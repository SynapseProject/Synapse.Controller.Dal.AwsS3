using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Suplex.Security;

using Synapse.Core;
using Synapse.Core.Utilities;
using Synapse.Services.Controller.Dal;

using zf = Zephyr.Filesystem;


public partial class AwsS3Dal : IControllerDal
{
    public bool HasAccess(string securityContext, string planUniqueName, FileSystemRight right = FileSystemRight.Execute)
    {
        bool ok = false;
        try
        {
            _splxDal?.TrySecurityOrException( securityContext, planUniqueName, AceType.FileSystem, right, "Plan" );
            ok = true;
        }
        catch { }

        return ok;
    }

    public bool HasAccess(string securityContext, string planUniqueName, AceType aceType, object right)
    {
        bool ok = false;
        try
        {
            _splxDal?.TrySecurityOrException( securityContext, planUniqueName, aceType, right, "Plan" );
            ok = true;
        }
        catch { }

        return ok;
    }

    public void HasAccessOrException(string securityContext, string planUniqueName, FileSystemRight right = FileSystemRight.Execute)
    {
        _splxDal?.TrySecurityOrException( securityContext, planUniqueName, AceType.FileSystem, right, "Plan" );
    }

    public void HasAccessOrException(string securityContext, string planUniqueName, AceType aceType, object right)
    {
        _splxDal?.TrySecurityOrException( securityContext, planUniqueName, aceType, right, "Plan" );
    }


    public IEnumerable<string> GetPlanList(string filter = null, bool isRegexFilter = true)
    {
        if( string.IsNullOrEmpty( filter ) )
        {
            return DirectoryGetFiles( _planPath, ".yaml" ).Select( f => Path.GetFileNameWithoutExtension( f ) );
        }
        else
        {
            if( !isRegexFilter )
            {
                foreach( char x in @"\+?|{[()^$.#" )
                    filter = filter.Replace( x.ToString(), @"\" + x.ToString() );
                filter = $@"{filter.Replace( "*", ".*" )}.*\.yaml$";
            }
            else if( !filter.EndsWith( ".yaml", StringComparison.OrdinalIgnoreCase ) )
            {
                if( filter.EndsWith( "$" ) )
                    filter = $@"{filter.Remove( filter.Length - 1 )}\.yaml$";
                else
                    filter = $@"{filter}.*\.yaml$";
            }

            Regex regex = new Regex( filter, RegexOptions.IgnoreCase );

            return DirectoryGetFiles( _planPath ).Where( f => regex.IsMatch( Path.GetFileName( f ) ) )
                .Select( f => Path.GetFileNameWithoutExtension( f ) );
        }
    }

    public IEnumerable<long> GetPlanInstanceIdList(string planUniqueName)
    {
        Regex regex = new Regex( $@"^{planUniqueName}(_\d+\{_histExt})$", RegexOptions.IgnoreCase );
        IEnumerable<string> files = DirectoryGetFiles( _histPath )
            .Where( f => regex.IsMatch( Path.GetFileName( f ) ) )
            .Select( f => Path.GetFileNameWithoutExtension( f ) );

        List<long> ids = new List<long>();
        foreach( string file in files )
        {
            Match m = Regex.Match( file, @"_(?<instanceId>\d+)" );
            string iid = m.Groups["instanceId"].Value;
            if( !string.IsNullOrWhiteSpace( iid ) )
                ids.Add( long.Parse( iid ) );
        }

        return ids;
    }

    public Plan GetPlan(string planUniqueName)
    {
        string planFile = DirectoryGetFile( _planPath, $"{planUniqueName}.yaml" );
        return DeserializeYamlFile<Plan>( planFile );
    }

    public Plan CreatePlanInstance(string planUniqueName)
    {
        Plan plan = GetPlan( planUniqueName );

        if( string.IsNullOrWhiteSpace( plan.UniqueName ) )
            plan.UniqueName = planUniqueName;
        plan.InstanceId = PlanInstanceIdCounter++;

        return plan;
    }

    public Plan GetPlanStatus(string planUniqueName, long planInstanceId)
    {
        string planFile = DirectoryGetFile( _histPath, $"{planUniqueName}_{planInstanceId}{_histExt}" );
        return DeserializeYamlFile<Plan>( planFile );
    }

    public void UpdatePlanStatus(Plan plan)
    {
        PlanUpdateItem item = new PlanUpdateItem() { Plan = plan };

        if( ProcessPlansOnSingleton )
            PlanItemSingletonProcessor.Instance.Queue.Enqueue( item );
        else
            UpdatePlanStatus( item );
    }

    public void UpdatePlanStatus(PlanUpdateItem item)
    {
        try
        {
            SerializeYamlFile( UtilitiesPathCombine( _histPath, $"{item.Plan.UniqueName}_{item.Plan.InstanceId}{_histExt}" ),
                item.Plan, serializeAsJson: _histAsJson, formatJson: _histAsFormattedJson, emitDefaultValues: true );
        }
        catch( Exception ex )
        {
            PlanItemSingletonProcessor.Instance.Exceptions.Enqueue( ex );

            if( item.RetryAttempts++ < 5 )
                PlanItemSingletonProcessor.Instance.Queue.Enqueue( item );
            else
                PlanItemSingletonProcessor.Instance.Fatal.Enqueue( ex );
        }
    }

    public void UpdatePlanActionStatus(string planUniqueName, long planInstanceId, ActionItem actionItem)
    {
        ActionUpdateItem item = new ActionUpdateItem()
        {
            PlanUniqueName = planUniqueName,
            PlanInstanceId = planInstanceId,
            ActionItem = actionItem
        };

        if( ProcessActionsOnSingleton )
            ActionItemSingletonProcessor.Instance.Queue.Enqueue( item );
        else
            UpdatePlanActionStatus( item );
    }

    public void UpdatePlanActionStatus(ActionUpdateItem item)
    {
        try
        {
            Plan plan = GetPlanStatus( item.PlanUniqueName, item.PlanInstanceId );
            bool ok = DalUtilities.FindActionAndReplace( plan.Actions, item.ActionItem );
            if( ok )
                SerializeYamlFile( UtilitiesPathCombine( _histPath, $"{plan.UniqueName}_{plan.InstanceId}{_histExt}" ),
                    plan, serializeAsJson: _histAsJson, formatJson: _histAsFormattedJson, emitDefaultValues: true );
            else
                throw new Exception( $"Could not find Plan.InstanceId = [{item.PlanInstanceId}], Action:{item.ActionItem.Name}.ParentInstanceId = [{item.ActionItem.ParentInstanceId}] in Plan outfile." );
        }
        catch( Exception ex )
        {
            ActionItemSingletonProcessor.Instance.Exceptions.Enqueue( ex );

            if( item.RetryAttempts++ < 5 )
                ActionItemSingletonProcessor.Instance.Queue.Enqueue( item );
            else
                ActionItemSingletonProcessor.Instance.Fatal.Enqueue( ex );
        }
    }


    #region utilities
    void SerializeYamlFile(string path, object data, bool serializeAsJson = false, bool formatJson = true, bool emitDefaultValues = false)
    {
        string yaml = YamlHelpers.Serialize( data, serializeAsJson: serializeAsJson, formatJson: formatJson, emitDefaultValues: true );
        zf.AwsS3ZephyrFile file = new zf.AwsS3ZephyrFile( _awsClient, path );
        file.WriteAllText( yaml );
    }

    T DeserializeYamlFile<T>(string path)
    {
        zf.AwsS3ZephyrFile file = new zf.AwsS3ZephyrFile( _awsClient, path );
        string yaml = file.ReadAllText();
        return YamlHelpers.Deserialize<T>( yaml );
    }

    /// <summary>
    /// Case-insensitive search a folder for an exact filename match
    /// </summary>
    /// <param name="path">Folder to search</param>
    /// <param name="fileName">Filename to match</param>
    /// <returns>The matching case-sensitive path or (returns null or throws FileNotFoundException)</returns>
    string DirectoryGetFile(string path, string fileName, bool throwFileNotFoundException = true)
    {
        zf.AwsS3ZephyrDirectory dir = new zf.AwsS3ZephyrDirectory( _awsClient, path );

        List<string> files = dir.GetFiles()
            .Where( f => f.Name.Equals( fileName, StringComparison.OrdinalIgnoreCase ) )
            .Select( f => f.Name ).ToList();

        if( files.Count == 1 )
            return UtilitiesPathCombine( path, files[0] );
        else if( throwFileNotFoundException )
            throw new FileNotFoundException( $"Could not load {fileName}.  Found {files.Count} name matches." );
        else
            return null;
    }

    /// <summary>
    /// Case-insensitive search a folder for a list of matching files
    /// </summary>
    /// <param name="path">Folder to search</param>
    /// <param name="searchPattern">Optional suffix to match</param>
    /// <returns>A list of matching files</returns>
    IEnumerable<string> DirectoryGetFiles(string path, string searchPattern = null)
    {
        zf.AwsS3ZephyrDirectory dir = new zf.AwsS3ZephyrDirectory( _awsClient, path );
        IEnumerable<zf.ZephyrFile> files = dir.GetFiles();
        List<string> matches = new List<string>();
        if( !string.IsNullOrWhiteSpace( searchPattern ) )
        {
            foreach( zf.ZephyrFile z in files )
                if( z.Name.EndsWith( searchPattern, StringComparison.OrdinalIgnoreCase ) )
                    matches.Add( z.FullName );
        }
        else
            foreach( zf.ZephyrFile z in files )
                matches.Add( z.FullName );

        return matches;
    }

    string UtilitiesPathCombine(params string[] paths)
    {
        zf.AwsS3ZephyrDirectory dir = new zf.AwsS3ZephyrDirectory( _awsClient, paths[0] );
        return dir.PathCombine( paths );
    }
    #endregion
}