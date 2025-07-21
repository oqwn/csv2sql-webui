import axios from 'axios';
import {
  TransformationPipeline,
  TransformationStep,
  TransformationPreviewRequest,
  TransformationPreviewResponse,
  TransformationExecuteRequest,
  TransformationExecuteResponse,
} from '../types/transformation.types';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

class TransformationService {
  async createPipeline(pipeline: TransformationPipeline): Promise<any> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/transformations/pipelines`, pipeline);
    return response.data;
  }

  async listPipelines(): Promise<{ pipelines: TransformationPipeline[]; total: number }> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/transformations/pipelines`);
    return response.data;
  }

  async getPipeline(pipelineId: string): Promise<TransformationPipeline> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/transformations/pipelines/${pipelineId}`);
    return response.data;
  }

  async updatePipeline(pipelineId: string, pipeline: TransformationPipeline): Promise<any> {
    const response = await axios.put(
      `${API_BASE_URL}/api/v1/transformations/pipelines/${pipelineId}`,
      pipeline
    );
    return response.data;
  }

  async deletePipeline(pipelineId: string): Promise<any> {
    const response = await axios.delete(`${API_BASE_URL}/api/v1/transformations/pipelines/${pipelineId}`);
    return response.data;
  }

  async previewTransformation(
    sourceConfig: any,
    steps: TransformationStep[],
    previewRows: number = 100
  ): Promise<TransformationPreviewResponse> {
    const request: TransformationPreviewRequest = {
      source_config: sourceConfig,
      steps,
      preview_rows: previewRows,
    };
    const response = await axios.post(`${API_BASE_URL}/api/v1/transformations/preview`, request);
    return response.data;
  }

  async executePipeline(
    pipelineId: string,
    outputConfig: any
  ): Promise<TransformationExecuteResponse> {
    const request: TransformationExecuteRequest = {
      pipeline_id: pipelineId,
      output_config: outputConfig,
    };
    const response = await axios.post(`${API_BASE_URL}/api/v1/transformations/execute`, request);
    return response.data;
  }

  async executeTransformation(
    sourceConfig: any,
    steps: TransformationStep[],
    outputConfig: any
  ): Promise<TransformationExecuteResponse> {
    const request: TransformationExecuteRequest = {
      source_config: sourceConfig,
      steps,
      output_config: outputConfig,
    };
    const response = await axios.post(`${API_BASE_URL}/api/v1/transformations/execute`, request);
    return response.data;
  }

  async validateScript(scriptType: 'python' | 'sql', script: string): Promise<any> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/transformations/validate-script`, {
      script_type: scriptType,
      script,
    });
    return response.data;
  }

  async getAvailableFunctions(): Promise<any> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/transformations/functions`);
    return response.data;
  }
}

export const transformationService = new TransformationService();