"""
Data Quality Management API endpoints
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, File, UploadFile
from pydantic import BaseModel
import pandas as pd
import io

from app.services.data_quality_manager import (
    data_quality_manager, DataQualitySeverity, 
    NotNullRule, DataTypeRule, RangeRule, UniqueRule
)
from app.api.deps import get_current_user

router = APIRouter()


class ValidationRuleRequest(BaseModel):
    """Request model for creating validation rules"""
    rule_type: str  # "not_null", "data_type", "range", "unique"
    rule_name: str
    severity: str = "medium"
    parameters: Dict[str, Any]


class DatasetValidationRequest(BaseModel):
    """Request model for dataset validation"""
    dataset_id: Optional[str] = None
    isolation_strategy: str = "quarantine"  # "quarantine", "fix_and_continue", "ignore"


class ValidationProfileRequest(BaseModel):
    """Request model for creating validation profile"""
    sample_size: int = 1000


@router.post("/rules")
async def add_validation_rule(
    request: ValidationRuleRequest,
    current_user: dict = Depends(get_current_user)
):
    """Add a data quality validation rule"""
    try:
        severity = DataQualitySeverity(request.severity)
        
        if request.rule_type == "not_null":
            columns = request.parameters.get("columns", [])
            rule = NotNullRule(columns, severity)
            
        elif request.rule_type == "data_type":
            column_types = request.parameters.get("column_types", {})
            rule = DataTypeRule(column_types, severity)
            
        elif request.rule_type == "range":
            column_ranges = request.parameters.get("column_ranges", {})
            rule = RangeRule(column_ranges, severity)
            
        elif request.rule_type == "unique":
            columns = request.parameters.get("columns", [])
            rule = UniqueRule(columns, severity)
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported rule type: {request.rule_type}")
        
        data_quality_manager.add_rule(rule)
        
        return {
            "message": "Validation rule added successfully",
            "rule_id": rule.rule_id,
            "rule_type": request.rule_type,
            "severity": request.severity
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add rule: {str(e)}")


@router.delete("/rules/{rule_id}")
async def remove_validation_rule(
    rule_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Remove a data quality validation rule"""
    try:
        data_quality_manager.remove_rule(rule_id)
        return {"message": "Validation rule removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to remove rule: {str(e)}")


@router.get("/rules")
async def list_validation_rules(
    current_user: dict = Depends(get_current_user)
):
    """List all configured validation rules"""
    try:
        rules_info = []
        for rule in data_quality_manager.rules:
            rules_info.append({
                "rule_id": rule.rule_id,
                "name": rule.name,
                "severity": rule.severity.value,
                "type": type(rule).__name__
            })
        
        return {
            "rules": rules_info,
            "count": len(rules_info)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list rules: {str(e)}")


@router.post("/validate-file")
async def validate_uploaded_file(
    file: UploadFile = File(...),
    request: DatasetValidationRequest = Depends(),
    current_user: dict = Depends(get_current_user)
):
    """Validate an uploaded CSV file"""
    try:
        # Read uploaded file
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        
        # Validate dataset
        report = data_quality_manager.validate_dataset(df, request.dataset_id)
        
        # Isolate dirty data if requested
        clean_df, dirty_df = data_quality_manager.isolate_dirty_data(
            df, report, request.isolation_strategy
        )
        
        # Prepare response
        issues_summary = {}
        for issue in report.issues:
            issue_type = issue.issue_type.value
            if issue_type not in issues_summary:
                issues_summary[issue_type] = 0
            issues_summary[issue_type] += 1
        
        return {
            "dataset_id": report.dataset_id,
            "validation_summary": {
                "total_rows": report.total_rows,
                "total_columns": report.total_columns,
                "clean_rows": len(clean_df),
                "dirty_rows": len(dirty_df),
                "issues_count": len(report.issues),
                "issues_by_type": issues_summary
            },
            "issues": [
                {
                    "issue_id": issue.issue_id,
                    "type": issue.issue_type.value,
                    "severity": issue.severity.value,
                    "column": issue.column_name,
                    "row": issue.row_index,
                    "message": issue.message,
                    "value": issue.value
                }
                for issue in report.issues[:100]  # Limit to first 100 issues
            ],
            "has_more_issues": len(report.issues) > 100,
            "isolation_strategy": request.isolation_strategy
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate file: {str(e)}")


@router.post("/validate-dataset")
async def validate_dataset_from_data(
    data: Dict[str, Any],
    request: DatasetValidationRequest = Depends(),
    current_user: dict = Depends(get_current_user)
):
    """Validate a dataset from JSON data"""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data.get("rows", []))
        if data.get("columns"):
            df.columns = data["columns"]
        
        # Validate dataset
        report = data_quality_manager.validate_dataset(df, request.dataset_id)
        
        # Isolate dirty data if requested
        clean_df, dirty_df = data_quality_manager.isolate_dirty_data(
            df, report, request.isolation_strategy
        )
        
        return {
            "dataset_id": report.dataset_id,
            "validation_summary": {
                "total_rows": report.total_rows,
                "total_columns": report.total_columns,
                "clean_rows": len(clean_df),
                "dirty_rows": len(dirty_df),
                "issues_count": len(report.issues)
            },
            "clean_data": {
                "columns": clean_df.columns.tolist(),
                "rows": clean_df.to_dict('records')
            },
            "dirty_data": {
                "columns": dirty_df.columns.tolist() if not dirty_df.empty else [],
                "rows": dirty_df.to_dict('records') if not dirty_df.empty else []
            },
            "issues": [
                {
                    "type": issue.issue_type.value,
                    "severity": issue.severity.value,
                    "column": issue.column_name,
                    "row": issue.row_index,
                    "message": issue.message
                }
                for issue in report.issues
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate dataset: {str(e)}")


@router.post("/create-profile")
async def create_validation_profile(
    file: UploadFile = File(...),
    request: ValidationProfileRequest = Depends(),
    current_user: dict = Depends(get_current_user)
):
    """Create automatic validation profile from uploaded dataset"""
    try:
        # Read uploaded file
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        
        # Create validation profile
        rules = data_quality_manager.create_validation_profile(df, request.sample_size)
        
        # Add rules to manager
        for rule in rules.values():
            data_quality_manager.add_rule(rule)
        
        return {
            "message": f"Created validation profile with {len(rules)} rules",
            "rules_created": [
                {
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "type": type(rule).__name__,
                    "severity": rule.severity.value
                }
                for rule in rules.values()
            ],
            "sample_size": min(request.sample_size, len(df)),
            "dataset_shape": [len(df), len(df.columns)]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create profile: {str(e)}")


@router.get("/quarantine")
async def get_quarantine_data(
    quarantine_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Get quarantined dirty data"""
    try:
        quarantine_data = data_quality_manager.get_quarantine_data(quarantine_id)
        
        if quarantine_id:
            if not quarantine_data:
                raise HTTPException(status_code=404, detail="Quarantine data not found")
            return {
                "quarantine_id": quarantine_id,
                "data": quarantine_data
            }
        else:
            return {
                "quarantine_entries": [
                    {
                        "quarantine_id": qid,
                        "timestamp": data["timestamp"],
                        "dataset_id": data["dataset_id"],
                        "dirty_records_count": len(data["data"]),
                        "issues_count": len(data["issues"])
                    }
                    for qid, data in quarantine_data.items()
                ],
                "total_entries": len(quarantine_data)
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get quarantine data: {str(e)}")


@router.post("/export-report")
async def export_quality_report(
    dataset_id: str,
    format: str = "json",
    current_user: dict = Depends(get_current_user)
):
    """Export data quality report"""
    try:
        # This would need to retrieve the report from storage
        # For now, return a placeholder
        return {
            "message": "Export functionality not fully implemented",
            "dataset_id": dataset_id,
            "format": format
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export report: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "rules_count": len(data_quality_manager.rules),
        "quarantine_entries": len(data_quality_manager.quarantine_data)
    }