using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Suplex.Security;

using Synapse.Core;
using Synapse.Core.Utilities;
using Synapse.Services;
using Synapse.Services.Controller.Dal;

using zf = Zephyr.Filesystem;


//namespace Synapse.Services.Controller.Dal { }
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
        Regex regex = new Regex( $@"^{planUniqueName}(_\d+\.yaml)$", RegexOptions.IgnoreCase );
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
        string planFile = UtilitiesPathCombine( _planPath, $"{planUniqueName}.yaml" );
        return DeserializeYamlFile<Plan>( planFile );
    }

    public Plan CreatePlanInstance(string planUniqueName)
    {
        string planFile = UtilitiesPathCombine( _planPath, $"{planUniqueName}.yaml" );
        Plan plan = DeserializeYamlFile<Plan>( planFile );

        if( string.IsNullOrWhiteSpace( plan.UniqueName ) )
            plan.UniqueName = planUniqueName;
        plan.InstanceId = PlanInstanceIdCounter++;

        return plan;
    }

    public Plan GetPlanStatus(string planUniqueName, long planInstanceId)
    {
        string planFile = UtilitiesPathCombine( _histPath, $"{planUniqueName}_{planInstanceId}.yaml" );
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
            SerializeYamlFile( UtilitiesPathCombine( _histPath, $"{item.Plan.UniqueName}_{item.Plan.InstanceId}.yaml" ),
                item.Plan, serializeAsJson: _histAsJson, emitDefaultValues: true );
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
                SerializeYamlFile( UtilitiesPathCombine( _histPath, $"{plan.UniqueName}_{plan.InstanceId}.yaml" ), plan, emitDefaultValues: true );
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
        string yaml = YamlHelpers.Serialize( data, serializeAsJson: serializeAsJson, emitDefaultValues: true );
        zf.AwsS3ZephyrFile s3zf = new zf.AwsS3ZephyrFile( _awsClient, path );
        s3zf.WriteAllText( yaml );
    }

    T DeserializeYamlFile<T>(string path)
    {
        zf.AwsS3ZephyrFile s3zf = new zf.AwsS3ZephyrFile( _awsClient, path );
        string yaml = s3zf.ReadAllText();
        return YamlHelpers.Deserialize<T>( yaml );
    }

    IEnumerable<string> DirectoryGetFiles(string path, string searchPattern = null)
    {
        zf.AwsS3ZephyrDirectory s3zd = new zf.AwsS3ZephyrDirectory( _awsClient, path );
        IEnumerable<zf.ZephyrFile> zfs = s3zd.GetFiles();
        List<string> files = new List<string>();
        if( !string.IsNullOrWhiteSpace( searchPattern ) )
        {
            foreach( zf.ZephyrFile z in zfs )
                if( z.Name.EndsWith( searchPattern, StringComparison.OrdinalIgnoreCase ) )
                    files.Add( z.FullName );
        }
        else
            foreach( zf.ZephyrFile z in zfs )
                files.Add( z.FullName );

        return files;
    }

    string UtilitiesPathCombine(params string[] paths)
    {
        zf.AwsS3ZephyrDirectory splxFolder = new zf.AwsS3ZephyrDirectory( _awsClient, paths[0] );
        return splxFolder.PathCombine( paths );
    }
    #endregion
}