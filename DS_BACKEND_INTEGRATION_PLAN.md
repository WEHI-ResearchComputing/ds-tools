# DS-Backend Integration Plan for DS-Tools

This document outlines the implementation plan for integrating `ds-tools` functionality into `ds-backend` via RPC-like API routes.

## Overview

The integration will provide server-side storage operations through REST endpoints, allowing the `ds-portal` frontend to perform storage analysis, listing, and access verification across local, SSH, and S3 storage backends without direct client-side access to storage credentials or systems.

## Architecture

### Domain Structure
Following ds-backend patterns, create a new domain module:

```
src/ds_backend/tools/
├── __init__.py
├── models.py          # Optional: audit logging models
├── router.py          # FastAPI route definitions
├── schemas.py         # Pydantic request/response models
├── service.py         # Business logic using ds-tools
└── exceptions.py      # Domain-specific exceptions
```

### Route Organization
- **Prefix**: `/v1/tools`
- **Pattern**: Action-based endpoints with unified request/response formats
- **Authentication**: All routes require valid JWT tokens
- **Authorization**: Role-based access for sensitive operations

## API Design

### 1. Unified Storage Operations

#### **POST /v1/tools/storage/analyze**
Analyze storage metrics (file count, total size) across all storage types.

**Request Schema:**
```python
class StorageAnalyzeRequest(BaseModel):
    path: str = Field(..., description="Storage path (local, ssh://, or s3://)")
    # SSH configuration (optional)
    ssh_config: Optional[SSHConfigSchema] = None
    # S3 configuration (optional) 
    s3_config: Optional[S3ConfigSchema] = None
    # Operation settings
    timeout: int = Field(default=300, ge=1, le=3600)

class SSHConfigSchema(BaseModel):
    hostname: str
    username: str
    ssh_key_path: str = Field(..., description="Server-side path to SSH key")

class S3ConfigSchema(BaseModel):
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    region_name: str = "us-east-1"
    endpoint_url: Optional[str] = None
    aws_profile: Optional[str] = None
```

**Response Schema:**
```python
class StorageMetricsResponse(BaseModel):
    location: str
    storage_type: Literal["local", "ssh", "s3"]
    item_count: int = Field(..., ge=0)
    total_bytes: int = Field(..., ge=0)
    human_readable_size: str
    analysis_duration_ms: int
    trace_id: str
```

#### **POST /v1/tools/storage/list**
List storage contents (subdirectories/prefixes or files/objects).

**Request Schema:**
```python
class StorageListRequest(BaseModel):
    path: str
    content_type: Literal["subdirectories", "files"] = "subdirectories"
    max_items: int = Field(default=1000, ge=1, le=10000)
    # SSH/S3 configs same as above
    ssh_config: Optional[SSHConfigSchema] = None
    s3_config: Optional[S3ConfigSchema] = None
    timeout: int = Field(default=300, ge=1, le=3600)
```

**Response Schema:**
```python
class StorageListResponse(BaseModel):
    location: str
    storage_type: Literal["local", "ssh", "s3"]
    content_type: Literal["subdirectories", "files"]
    items: List[str]
    total_items: int
    truncated: bool = Field(description="True if results were truncated due to max_items limit")
    trace_id: str
```

#### **POST /v1/tools/storage/verify-access**
Verify storage access permissions.

**Request Schema:**
```python
class StorageAccessRequest(BaseModel):
    path: str
    operation: Literal["read", "write"] = "read"
    username: Optional[str] = None  # For filesystem operations
    # SSH/S3 configs same as above
    ssh_config: Optional[SSHConfigSchema] = None
    s3_config: Optional[S3ConfigSchema] = None
```

**Response Schema:**
```python
class StorageAccessResponse(BaseModel):
    location: str
    storage_type: Literal["local", "ssh", "s3"]
    operation: Literal["read", "write"]
    has_access: bool
    verification_method: str = Field(description="Method used for verification")
    details: Optional[str] = Field(description="Additional details or error info")
    trace_id: str
```

### 2. Specialized Operations

#### **POST /v1/tools/filesystem/operations**
Filesystem-specific operations for local and SSH storage.

**Request Schema:**
```python
class FilesystemOperationRequest(BaseModel):
    action: Literal["analyze", "list_subdirs", "verify_permissions"]
    path: str
    # SSH configuration for remote operations
    ssh_config: Optional[SSHConfigSchema] = None
    # Action-specific parameters
    username: Optional[str] = None  # For permission verification
    timeout: int = Field(default=300, ge=1, le=3600)
```

#### **POST /v1/tools/s3/operations**
S3-specific operations with enhanced S3 features.

**Request Schema:**
```python
class S3OperationRequest(BaseModel):
    action: Literal["analyze_prefix", "list_objects", "list_prefixes", "verify_access"]
    bucket: str
    prefix: str = ""
    s3_config: S3ConfigSchema
    # Action-specific parameters
    max_keys: Optional[int] = Field(default=1000, ge=1, le=10000)
    delimiter: str = "/"
    operation: Optional[Literal["read", "write"]] = "read"  # For access verification
```

### 3. Batch Operations

#### **POST /v1/tools/batch/analyze**
Analyze multiple storage locations in a single request.

**Request Schema:**
```python
class BatchAnalyzeRequest(BaseModel):
    operations: List[StorageAnalyzeRequest] = Field(..., max_length=10)
    fail_fast: bool = Field(default=False, description="Stop on first error")
```

**Response Schema:**
```python
class BatchAnalyzeResponse(BaseModel):
    results: List[Union[StorageMetricsResponse, ErrorDetail]]
    total_operations: int
    successful_operations: int
    failed_operations: int
    trace_id: str

class ErrorDetail(BaseModel):
    operation_index: int
    error: str
    details: Optional[str] = None
```

## Implementation Details

### Service Layer

```python
# src/ds_backend/tools/service.py
from ds_tools import analyze_storage, list_storage_contents, verify_storage_access
from ds_tools.storage_config import StorageConfig

class ToolsService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.logger = get_logger(__name__)
    
    async def analyze_storage(self, request: StorageAnalyzeRequest) -> StorageMetricsResponse:
        """Analyze storage metrics using ds-tools."""
        start_time = time.time()
        
        try:
            # Convert request to ds-tools parameters
            kwargs = self._build_storage_kwargs(request)
            
            # Call ds-tools unified interface
            metrics = analyze_storage(path=request.path, **kwargs)
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            return StorageMetricsResponse(
                location=metrics.location,
                storage_type=metrics.storage_type,
                item_count=metrics.item_count,
                total_bytes=metrics.total_bytes,
                human_readable_size=self._format_bytes(metrics.total_bytes),
                analysis_duration_ms=duration_ms,
                trace_id=get_current_trace_id()
            )
            
        except Exception as e:
            self.logger.error(
                "Storage analysis failed",
                path=request.path,
                error=str(e),
                **get_request_context()
            )
            raise BusinessException(f"Storage analysis failed: {str(e)}")
    
    def _build_storage_kwargs(self, request) -> dict:
        """Convert request schemas to ds-tools parameters."""
        kwargs = {"timeout": request.timeout}
        
        if request.ssh_config:
            kwargs.update({
                "hostname": request.ssh_config.hostname,
                "username": request.ssh_config.username,
                "ssh_key": request.ssh_config.ssh_key_path,
            })
        
        if request.s3_config:
            kwargs.update({
                "access_key_id": request.s3_config.access_key_id,
                "secret_access_key": request.s3_config.secret_access_key,
                "session_token": request.s3_config.session_token,
                "region_name": request.s3_config.region_name,
                "endpoint_url": request.s3_config.endpoint_url,
                "aws_profile": request.s3_config.aws_profile,
            })
        
        return kwargs
    
    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"
```

### Router Implementation

```python
# src/ds_backend/tools/router.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated

from ds_backend.core.auth import valid_access_token
from ds_backend.core.db import get_db_session
from ds_backend.core.exceptions import BusinessException

from .schemas import *
from .service import ToolsService

router = APIRouter(
    prefix="/v1/tools",
    tags=["tools"],
)

@router.post(
    "/storage/analyze",
    response_model=StorageMetricsResponse,
    summary="Analyze storage metrics",
    description="Calculate file/object count and total size for any storage type"
)
async def analyze_storage_endpoint(
    request: StorageAnalyzeRequest,
    token_data: Annotated[dict, Depends(valid_access_token)],
    db: AsyncSession = Depends(get_db_session)
) -> StorageMetricsResponse:
    """Analyze storage metrics across local, SSH, and S3 storage."""
    service = ToolsService(db)
    
    try:
        return await service.analyze_storage(request)
    except BusinessException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post(
    "/storage/list",
    response_model=StorageListResponse,
    summary="List storage contents",
    description="List subdirectories/prefixes or files/objects for any storage type"
)
async def list_storage_endpoint(
    request: StorageListRequest,
    token_data: Annotated[dict, Depends(valid_access_token)],
    db: AsyncSession = Depends(get_db_session)
) -> StorageListResponse:
    """List storage contents across local, SSH, and S3 storage."""
    service = ToolsService(db)
    return await service.list_storage_contents(request)

@router.post(
    "/storage/verify-access",
    response_model=StorageAccessResponse,
    summary="Verify storage access",
    description="Verify read/write access permissions for any storage type"
)
async def verify_access_endpoint(
    request: StorageAccessRequest,
    token_data: Annotated[dict, Depends(valid_access_token)],
    db: AsyncSession = Depends(get_db_session)
) -> StorageAccessResponse:
    """Verify storage access permissions."""
    service = ToolsService(db)
    return await service.verify_storage_access(request)
```

### Security and Configuration

#### **SSH Key Management**
- Store SSH keys on the server filesystem
- Use absolute paths in SSH configuration
- Implement key rotation and access auditing
- Validate key file permissions (600/400)

#### **S3 Credentials**
- Support multiple authentication methods:
  - IAM roles (preferred for production)
  - AWS profiles for development
  - Explicit credentials (secure storage required)
- Use environment variables for default configurations
- Implement credential validation before operations

#### **Access Control**
- All endpoints require authentication
- Consider role-based authorization for sensitive operations
- Log all storage operations for audit trails
- Rate limiting for resource-intensive operations

### Error Handling

```python
# src/ds_backend/tools/exceptions.py
class ToolsException(BusinessException):
    """Base exception for tools operations."""
    pass

class StorageConnectionError(ToolsException):
    """Failed to connect to storage backend."""
    pass

class StoragePermissionError(ToolsException):
    """Insufficient permissions for storage operation."""
    pass

class StorageTimeoutError(ToolsException):
    """Storage operation timed out."""
    pass
```

### Observability

- **Structured Logging**: Log all operations with trace IDs
- **Metrics**: Track operation duration, success/failure rates
- **Tracing**: Instrument ds-tools operations for observability
- **Health Checks**: Verify connectivity to common storage backends

## Frontend Integration

### React Hooks Examples

```typescript
// hooks/use-storage-operations.ts
export function useStorageAnalysis() {
  return useMutation({
    mutationFn: async (request: StorageAnalyzeRequest) => {
      const response = await fetch('/api/v1/tools/storage/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
      });
      return response.json();
    },
  });
}

export function useStorageListing() {
  return useMutation({
    mutationFn: async (request: StorageListRequest) => {
      const response = await fetch('/api/v1/tools/storage/list', {
        method: 'POST',
        body: JSON.stringify(request),
      });
      return response.json();
    },
  });
}
```

### Component Usage

```tsx
// components/storage-browser.tsx
export function StorageBrowser({ path }: { path: string }) {
  const listMutation = useStorageListing();
  const analysisMutation = useStorageAnalysis();
  
  const handleAnalyze = () => {
    analysisMutation.mutate({ path });
  };
  
  const handleList = () => {
    listMutation.mutate({ 
      path, 
      content_type: 'subdirectories',
      max_items: 100 
    });
  };
  
  return (
    <div>
      <button onClick={handleAnalyze}>Analyze Storage</button>
      <button onClick={handleList}>List Contents</button>
      {/* Results display */}
    </div>
  );
}
```

## Implementation Phases

### Phase 1: Core Integration
1. Create tools domain module structure
2. Implement unified storage operations (analyze, list, verify-access)
3. Add basic error handling and logging
4. Write unit and integration tests

### Phase 2: Enhanced Features
1. Add specialized filesystem and S3 operations
2. Implement batch operations
3. Add operation caching for performance
4. Enhance security and access control

### Phase 3: Optimization
1. Add background task support for long-running operations
2. Implement result streaming for large listings
3. Add operation scheduling and queuing
4. Performance monitoring and optimization

## Testing Strategy

### Unit Tests
- Test service layer with mocked ds-tools
- Test schema validation and serialization
- Test error handling and edge cases

### Integration Tests
- Test with real storage backends (using test data)
- Test authentication and authorization
- Test end-to-end API flows

### Performance Tests
- Load testing for concurrent operations
- Memory usage profiling for large listings
- Timeout handling validation

## Dependencies

### Backend Dependencies
- Add `ds-tools` to ds-backend dependencies
- Ensure compatible Python versions
- Handle ds-tools configuration in settings

### Infrastructure Requirements
- SSH key management and storage
- S3 credentials configuration
- Network access to storage backends
- Monitoring and logging infrastructure

This plan provides a comprehensive approach to integrating ds-tools into ds-backend while following established patterns and maintaining security, observability, and maintainability standards.